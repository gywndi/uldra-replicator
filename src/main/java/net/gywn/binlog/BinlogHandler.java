/*
 * Copyright 2021 Dongchan Sung (gywndi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.gywn.binlog;

import java.io.Serializable;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogColumn;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTable;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;
import net.gywn.binlog.handler.OperationBinlogHandler;

@Getter
@ToString
public class BinlogHandler {
	private static final Logger logger = LoggerFactory.getLogger(BinlogHandler.class);
	private final Map<Integer, BinlogTransaction> miniTransactions = new HashMap<Integer, BinlogTransaction>();

	private UldraConfig uldraConfig;
	private BinaryLogClient binaryLogClient = null;
	private BinlogHandlerWorker[] binlogHandlerWorkers;

	private Long workingBinlogPosition = 0L;
	private Long lastBinlogFlushTimeMillis = 0L;

	private Map<Long, BinlogTable> binlogTableMap = new HashMap<Long, BinlogTable>();
	private Calendar time = Calendar.getInstance();
	private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private Exception threadException;
	private BinlogTransaction binlogTransaction = null;

	private boolean threadRunning = false;

	// used for checking group key changed in binlog transaction level
	private boolean groupKeyChanged = false;

	@Setter
	private Binlog currntBinlog;
	@Setter
	private Binlog targetBinlog;
	@Setter
	private boolean recovering = true;

	public BinlogHandler(final UldraConfig uldraConfig) {
		this.uldraConfig = uldraConfig;

		// =========================
		// Create transaction worker
		// =========================
		binlogHandlerWorkers = new BinlogHandlerWorker[uldraConfig.getWorkerCount()];
		for (int i = 0; i < binlogHandlerWorkers.length; i++) {
			try {
				binlogHandlerWorkers[i] = new BinlogHandlerWorker(i, uldraConfig);
				binlogHandlerWorkers[i].start();
			} catch (Exception e) {
				logger.error("Start binlog event worker[{}] failed - {}", i, e.getMessage());
				System.exit(1);
			}
		}
	}

	public void receiveWriteRowsEvent(final Event event) {
		WriteRowsEventData eventData = (WriteRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		if (!binlogTable.isTarget()) {
			return;
		}

		BitSet bit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Serializable[] row : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, uldraConfig, OperationBinlogHandler.INS);

			// =====================
			// New image
			// =====================
			int seq = -1;
			for (Serializable serializable : row) {
				seq = bit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getDatMap().put(key, value);
				}
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

			binlogTransaction.addOperation(binlogOperation);
		}
	}

	public void receiveUpdateRowsEvent(final Event event) {
		UpdateRowsEventData eventData = (UpdateRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		if (!binlogTable.isTarget()) {
			return;
		}

		int seq;
		BitSet oldBit = eventData.getIncludedColumnsBeforeUpdate();
		BitSet newBit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Entry<Serializable[], Serializable[]> entry : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, uldraConfig, OperationBinlogHandler.UPD);

			// =====================
			// New image
			// =====================
			seq = -1;
			for (Serializable serializable : entry.getValue()) {
				seq = newBit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getDatMap().put(key, value);
				}
			}

			// =====================
			// Old image
			// =====================
			seq = -1;
			String groupValue = null;
			for (Serializable serializable : entry.getKey()) {
				seq = oldBit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null && column.isRowKey()) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getKeyMap().put(key, value);

					// check group key
					if (binlogTable.getBinlogPolicy().getGroupKey() != null) {
						if (key.equalsIgnoreCase(binlogTable.getBinlogPolicy().getGroupKey())) {
							groupValue = value;

							// check group key has been changed
							if (binlogOperation.getDatMap().containsKey(key)) {
								String groupAfterValue = binlogOperation.getDatMap().get(key);
								if (groupValue == null) {
									groupValue = groupAfterValue;
								}

								if (groupValue != null && !groupValue.equals(groupAfterValue)) {
									logger.debug("Partition key changed, `{}`->`{}`", value, groupAfterValue);
									binlogOperation.setGroupKeyChanged(true);
									groupKeyChanged = true;
								}
							}
						}
					}
				}
			}

			binlogOperation.setCrc32Code(UldraUtil.crc32(groupValue));
			logger.debug("binlogOperation - {}", binlogOperation);

			binlogTransaction.addOperation(binlogOperation);

		}
	}

	public void receiveDeleteRowsEvent(final Event event) {
		DeleteRowsEventData eventData = (DeleteRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		if (!binlogTable.isTarget()) {
			return;
		}

		BitSet bit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Serializable[] row : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, uldraConfig, OperationBinlogHandler.DEL);

			// =====================
			// Delete image
			// =====================
			int seq = -1;
			for (Serializable serializable : row) {
				seq = bit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null && column.isRowKey()) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getKeyMap().put(key, value);
				}
			}

			String groupValue = null;
			if (binlogTable.getBinlogPolicy().getGroupKey() != null) {
				groupValue = binlogOperation.getKeyMap().get(binlogTable.getBinlogPolicy().getGroupKey());
				binlogOperation.setCrc32Code(UldraUtil.crc32(groupValue));
			}

			logger.debug("binlogOperation - {}", binlogOperation);

			binlogTransaction.addOperation(binlogOperation);
		}
	}

	public void receiveTableMapEvent(final Event event) {
		TableMapEventData eventData = (TableMapEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());
		if (binlogTable != null && binlogTable.equalsTable(eventData)) {
			return;
		}

		logger.info("Meta info for TABLE_ID_{} is not in cache (`{}`.`{}`)", eventData.getTableId(),
				eventData.getDatabase(), eventData.getTable());

		binlogTable = BinlogTable.getBinlogTable(uldraConfig, eventData.getDatabase().toLowerCase(),
				eventData.getTable().toLowerCase());

		// cache
		binlogTableMap.put(eventData.getTableId(), binlogTable);

		if (!binlogTable.isTarget()) {
			logger.info("Skip `{}`.`{}`, not target", eventData.getDatabase(), eventData.getTable());
			return;
		}

		logger.info("Remove legacy meta info for `{}`.`{}`", eventData.getDatabase(), eventData.getTable());
		for (Entry<Long, BinlogTable> entry : binlogTableMap.entrySet()) {
			if (entry.getKey() != eventData.getTableId() && entry.getValue().getName().equals(binlogTable.getName())) {
				binlogTableMap.remove(entry.getKey());
				break;
			}
		}
	}

	public void receiveRotateEvent(final Event event) {
		RotateEventData eventData = (RotateEventData) event.getData();
		currntBinlog.setBinlogFile(eventData.getBinlogFilename());
		currntBinlog.setBinlogPosition(eventData.getBinlogPosition());
		logger.info("Binlog rotated - {}", currntBinlog);
	}

	public void receiveQueryEvent(final Event event) {
		EventHeaderV4 header = event.getHeader();
		currntBinlog.setBinlogPosition(header.getPosition());

		QueryEventData eventData = (QueryEventData) event.getData();
		switch (eventData.getSql()) {
		case "BEGIN":
			transactionStart();
			break;
		case "COMMIT":
			transactionEnd();
			break;
		default:
			logger.debug(event.toString());
		}
	}

	public void receiveXidEvent(final Event event) {
		try {
			EventHeaderV4 header = event.getHeader();
			transactionEnd();
		} finally {
		}
	}

	private void transactionStart() {
		logger.debug("transactionStart =====>");

		if (binlogTransaction == null) {
			logger.debug("create binlogTransaction");
			binlogTransaction = new BinlogTransaction(currntBinlog.toString(), recovering);
		}
	}

	private void transactionEnd() {
		logger.debug("transactionEnd");

		try {

			// =======================================
			// Empty transaction
			// =======================================
			if (binlogTransaction.size() == 0) {
				logger.debug("No operation");
				return;
			}

			// =======================================
			// partiton key has been changed
			// =======================================
			if (groupKeyChanged) {
				logger.debug("Partition key changed, single transaction processiong");
				waitJobProcessing();
				binlogHandlerWorkers[0].enqueue(binlogTransaction);
				waitJobProcessing();
				return;
			}

			// =======================================
			// single operation
			// =======================================
			if (binlogTransaction.size() == 1) {
				BinlogOperation binlogOperation = binlogTransaction.getBinlogOperations().get(0);
				int slot = (int) (binlogOperation.getCrc32Code() % uldraConfig.getWorkerCount());

				logger.debug("Single operation, slot {} - {}", slot, binlogOperation);
				binlogHandlerWorkers[slot].enqueue(binlogTransaction);
				return;
			}

			// =======================================
			// transction -> mini-trx by partition key
			// =======================================
			miniTransactions.clear();
			for (final BinlogOperation binlogOperation : binlogTransaction.getBinlogOperations()) {
				logger.debug("Partition key changed");
				int slot = (int) (binlogOperation.getCrc32Code() % uldraConfig.getWorkerCount());

				// new mini trx if not exists in trx map
				if (!miniTransactions.containsKey(slot)) {
					logger.debug("Create new mini-trx slot: {}", slot);
					miniTransactions.put(slot, new BinlogTransaction(binlogTransaction.getPosition(), recovering));
				}

				// add operation in mini trx
				logger.debug("Add operation in mini-trx slot {} - {}", slot, binlogOperation);
				miniTransactions.get(slot).addOperation(binlogOperation);
			}

			// ======================================
			// enqueue mini transactions
			// ======================================
			for (final Entry<Integer, BinlogTransaction> entry : miniTransactions.entrySet()) {
				logger.debug("Enqueue mini-trx {}", entry.getKey());
				binlogHandlerWorkers[entry.getKey()].enqueue(entry.getValue());
			}
		} finally {
			groupKeyChanged = false;
			binlogTransaction = null;
		}
	}

	public boolean isRecoveringPosition() {
		if (targetBinlog.compareTo(currntBinlog) > 0) {
			logger.debug("Recovering position [current]{} [target]{}", currntBinlog, targetBinlog);
			return true;
		}
		logger.debug("Recovering mode false");

		return false;
	}

	public Binlog getDatabaseBinlog() throws SQLException {
		Connection connection = null;
		Binlog binlogServerBinlog = null;
		try {
			connection = uldraConfig.getBinlogDataSource().getConnection();
			String query = "show master status";
			PreparedStatement pstmt = connection.prepareStatement(query);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				binlogServerBinlog = new Binlog(rs.getString("File"), rs.getLong("Position"));
				return binlogServerBinlog;
			}
			pstmt.close();
		} finally {
			logger.debug("Current binlog position from binlog server: {}", binlogServerBinlog);
			try {
				connection.close();
			} catch (Exception e) {
			}
		}
		return null;
	}

	// wait queue processing
	private void waitJobProcessing() {
		int sleepMS = 1;
		while (true) {

			if (getCurrentJobCount() == 0) {
				break;
			}

			logger.debug("Sleep {}ms", sleepMS);
			UldraUtil.sleep(sleepMS);
			sleepMS *= 2;
		}
	}

	public int getCurrentJobCount() {
		int currentJobs = 0;
		for (BinlogHandlerWorker worker : binlogHandlerWorkers) {
			currentJobs += worker.getJobCount();
		}

		logger.debug("Current remain jobs {}", currentJobs);
		return currentJobs;
	}

	public List<Binlog> getWorkerBinlogList() {
		List<Binlog> binlogList = new ArrayList<Binlog>();
		for (BinlogHandlerWorker binlogHandlerWorker : binlogHandlerWorkers) {
			Binlog binlog = binlogHandlerWorker.getLastExecutedBinlog();
			if (binlog != null) {
				binlogList.add(binlog);
			}
		}
		return binlogList;
	}
}