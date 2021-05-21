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
}
