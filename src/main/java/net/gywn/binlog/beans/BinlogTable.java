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

package net.gywn.binlog.beans;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import lombok.Getter;
import lombok.ToString;
import net.gywn.binlog.handler.RowHandler;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.ReplicatPolicy;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;

@Getter
@ToString
public class BinlogTable {
	private static final Logger logger = LoggerFactory.getLogger(BinlogTable.class);

	private final String name;
	private final List<BinlogColumn> columns;
	private final List<BinlogColumn> rowKeys;
	private final List<TargetTable> targetTables = new ArrayList<TargetTable>();
	private boolean target = true;
	private BinlogPolicy binlogPolicy;
	private RowHandler rowHandler;

	public BinlogTable(final String name, List<BinlogColumn> columns, List<BinlogColumn> rowKeys,
			final BinlogPolicy binlogPolicy) throws Exception {
		this.name = name;
		this.columns = columns;
		this.rowKeys = rowKeys;
		this.binlogPolicy = binlogPolicy;

		if (binlogPolicy == null) {
			logger.debug("{} is not target", name);
			this.target = false;
			return;
		}

		// ===========================================
		// Check group key for parallel processing
		// ===========================================
		if (binlogPolicy.getGroupKey() != null) {
			boolean isPrimaryKey = false;
			for (BinlogColumn column : rowKeys) {
				if (column.getName().equalsIgnoreCase(binlogPolicy.getGroupKey())) {
					logger.debug("grou key {} in primary key {}", column.getName(), binlogPolicy.getGroupKey());
					isPrimaryKey = true;
				}
			}

			if (!isPrimaryKey) {
				logger.info("{} group key {} is not in primary key, switching to sequential processing",
						binlogPolicy.getName(), binlogPolicy.getGroupKey());
			}
		}

		// ===========================================
		// Set replication policy in binlog table
		// ===========================================
		for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
			TargetTable targetTable = new TargetTable(this.columns, binlogPolicy.getGroupKey(), replicatPolicy);
			targetTables.add(targetTable);
		}

		// ===========================================
		// Set row handler in binlog table
		// ===========================================
		this.rowHandler = binlogPolicy.getRowHandler();

	}

	public boolean isTarget() {
		logger.debug("`{}` target is {}", this.name, this.target);
		return target;
	}

	public boolean equalsTable(final TableMapEventData tableMapEventData) {
		String eventTableName = getTableName(tableMapEventData.getDatabase(), tableMapEventData.getTable());
		if (!eventTableName.equalsIgnoreCase(this.name)) {
			logger.info("`{}` is not same with `{}` in table map event", this.name, eventTableName);
			return false;
		}
		return true;
	}

	public static String getTableName(final String database, final String table) {
		return String.format("%s.%s", database, table);
	}

	public static BinlogTable getBinlogTable(final UldraConfig uldraConfig, final String database, final String table) {
		String name = BinlogTable.getTableName(database, table);
		BinlogPolicy binlogPolicy = uldraConfig.getBinlogPolicyMap().get(name);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String query = null;
		while (true) {
			try {

				// Get connection
				connection = uldraConfig.getBinlogDataSource().getConnection();

				// Get columns
				List<BinlogColumn> columns = new ArrayList<BinlogColumn>();
				query = " select ordinal_position,";
				query += "  lower(column_name) column_name,";
				query += "  lower(character_set_name) character_set_name,";
				query += "  lower(data_type) data_type,";
				query += "  instr(column_type, 'unsigned') is_unsigned";
				query += " from information_schema.columns";
				query += " where table_schema = ?";
				query += " and table_name = ?";
				query += " order by ordinal_position";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name").toLowerCase();
					String columnCharset = rs.getString("character_set_name");
					String dataType = rs.getString("data_type");
					boolean columnUnsigned = rs.getBoolean("is_unsigned");
					columns.add(new BinlogColumn(columnName, dataType, columnCharset, columnUnsigned));
				}
				rs.close();
				pstmt.close();

				// Get primary key & unique key
				List<BinlogColumn> rowKeys = new ArrayList<BinlogColumn>();
				query = " select distinct ";
				query += "   column_name ";
				query += " from information_schema.table_constraints a ";
				query += " inner join information_schema.statistics b on b.table_schema = a.table_schema ";
				query += "   and a.table_name = b.table_name ";
				query += "   and b.index_name = a.constraint_name ";
				query += " where lower(a.constraint_type) in ('primary key') ";
				query += " and a.table_schema = ? ";
				query += " and a.table_name = ? ";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name").toLowerCase();
					for (BinlogColumn column : columns) {
						if (column.getName().equals(columnName)) {
							column.setRowKey(true);
							rowKeys.add(column);
							break;
						}
					}
				}
				rs.close();
				pstmt.close();

				return new BinlogTable(name, columns, rowKeys, binlogPolicy);

			} catch (Exception e) {
				logger.error(e.getMessage());
				UldraUtil.sleep(1000);
			} finally {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}
}
