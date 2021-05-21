package net.gywn.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.BinlogHandler;
import net.gywn.binlog.BinlogServer;
import net.gywn.binlog.Main;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogColumn;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTable;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;
import net.gywn.binlog.handler.OperationBinlogHandler;

public class BulkLoader {
	private static final Logger logger = LoggerFactory.getLogger(BulkLoader.class);
	private final UldraConfig uldraConfig;
	private final BinlogHandler binlogHandler;
	private final Map<Integer, BinlogTransaction> miniTransactions = new HashMap<Integer, BinlogTransaction>();
	private Binlog startBinlog = null;
	private Binlog endBinlog = null;
	private long processCount = 0;

	public BulkLoader(final BinlogServer binlogServer) throws SQLException {
		this.binlogHandler = binlogServer.getBinlogHandler();
		this.uldraConfig = binlogServer.getUldraConfig();
		this.startBinlog = binlogHandler.getDatabaseBinlog();
	}

	public void start() throws SQLException, IOException {
		for (BinlogPolicy binlogPolicy : uldraConfig.getBinlogPolicies()) {
			String[] names = binlogPolicy.getName().split("\\.");
			loadData(names[0], names[1]);
		}
		Binlog endBinlog = binlogHandler.getDatabaseBinlog();
		Binlog.flush(startBinlog, endBinlog, uldraConfig.getBinlogInfoFile());
	}

	public void loadData(final String database, final String table) throws SQLException, IOException {
		logger.info("Start migration for {}.{}", database, table);
		BinlogTable binlogTable = BinlogTable.getBinlogTable(uldraConfig, database, table);
		String sql = String.format("select * from %s.%s", database, table);
		logger.info("Migration query - {}", sql);

		// To avoid OOM
		Connection connection = uldraConfig.getBinlogDataSource().getConnection();
		Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsMeta = rs.getMetaData();
		String[] columns = new String[rsMeta.getColumnCount()];
		for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
			columns[i - 1] = rsMeta.getColumnLabel(i).toLowerCase();
		}

		miniTransactions.clear();
		while (rs.next()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, uldraConfig, OperationBinlogHandler.INS);

			for (String column : columns) {
				binlogOperation.getDatMap().put(column, rs.getString(column));
			}

			for (BinlogColumn column : binlogTable.getRowKeys()) {
				String key = column.getName();
				String value = binlogOperation.getDatMap().get(key);
				binlogOperation.getKeyMap().put(key, value);
			}

			String groupValue = null;
			if (binlogTable.getBinlogPolicy().getGroupKey() != null) {
				groupValue = binlogOperation.getKeyMap().get(binlogTable.getBinlogPolicy().getGroupKey());
				binlogOperation.setCrc32Code(UldraUtil.crc32(groupValue));
			}
			logger.debug("binlogOperation - {}", binlogOperation);

			int slot = (int) (binlogOperation.getCrc32Code() % uldraConfig.getWorkerCount());
			if (!miniTransactions.containsKey(slot)) {
				logger.debug("Create new mini-trx slot: {}", slot);
				miniTransactions.put(slot, new BinlogTransaction(startBinlog.toString(), true));
			}

			// add operation in mini trx
			logger.debug("Add operation in mini-trx slot {} - {}", slot, binlogOperation);
			miniTransactions.get(slot).addOperation(binlogOperation);

			// Enqueue data
			if (++processCount % uldraConfig.getWokerQueueSize() == 0) {
				flushTransactions();
			}

		}

		flushTransactions();

		rs.close();
		connection.close();
	}

	private void flushTransactions() {
		logger.debug("flush transactions {} slots", miniTransactions.size());
		for (Entry<Integer, BinlogTransaction> entry : miniTransactions.entrySet()) {
			binlogHandler.getBinlogHandlerWorkers()[entry.getKey()].enqueue(entry.getValue());
		}
		miniTransactions.clear();
	}

}
