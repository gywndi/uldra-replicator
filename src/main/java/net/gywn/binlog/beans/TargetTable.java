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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.ToString;
import net.gywn.binlog.common.ReplicatPolicy;
import net.gywn.binlog.handler.OperationTargetHandler;

@Getter
@ToString
public class TargetTable {
	private static final Logger logger = LoggerFactory.getLogger(TargetTable.class);

	private final String name;
	private final Map<String, String> columnMapper = new HashMap<String, String>();
	private final String groupKey;
	private OperationTargetHandler insert = OperationTargetHandler.INSERT;
	private OperationTargetHandler update = OperationTargetHandler.UPDATE;
	private OperationTargetHandler delete = OperationTargetHandler.DELETE;

	public TargetTable(final List<BinlogColumn> binlogColumns, final String groupKey,
			final ReplicatPolicy replicatPolicy) {

		this.name = replicatPolicy.getName();
		this.groupKey = groupKey;
		List<String> columns = replicatPolicy.getColums();

		// 타겟 칼럼이 지정안되었을 때, MySQL 칼럼 리스트 그대로 타겟을 지정하도록 세팅
		if (columns == null) {
			logger.info("Target columns not defined, set binlog colums");
			columns = new ArrayList<String>();
			for (BinlogColumn column : binlogColumns) {
				columns.add(column.getName());
			}
			replicatPolicy.setColums(columns);
		}
		logger.debug("{} Target columns {}", name, columns);

		// column map - ORIGIN_NAME:RENAME_NAME
		for (String column : columns) {
			String[] columnSplit = column.toLowerCase().split(":");
			columnMapper.put(columnSplit[0], columnSplit.length > 1 ? columnSplit[1] : columnSplit[0]);
		}
		logger.debug("{} Target column mapper {}", name, columnMapper);

		// change upsert
		if (replicatPolicy.isUpsertMode()) {
			logger.debug("{} set upsert mode}", name, columnMapper);
			insert = OperationTargetHandler.UPSERT;
		}

		// change soft delete
		if (replicatPolicy.isSoftDelete()) {
			logger.debug("{} set soft delete mode}", name, columnMapper);
			delete = OperationTargetHandler.SOFT_DELETE;
		}

		logger.info("TargetTable {}", this);
	}

	public Map<String, String> getTargetDataMap(final Map<String, String> map) {
		logger.debug("getTargetDataMap()");

		Map<String, String> resultMap = new HashMap<String, String>();

		// copy binlog data target columns only
		for (Entry<String, String> entry : columnMapper.entrySet()) {
			String column = entry.getKey();
			String columnNew = entry.getValue();
			if (map.containsKey(entry.getKey())) {
				resultMap.put(columnNew, map.get(column));
			}
		}

		logger.debug("Target data map {}", resultMap);
		return resultMap;
	}
}
