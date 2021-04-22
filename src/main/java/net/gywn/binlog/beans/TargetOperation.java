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

import java.util.Map;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class TargetOperation {
	private final String tableName;
	private final Map<String, String> datMap;
	private final Map<String, String> keyMap;
	private final TargetTable targetTable;
	private final boolean isGroupKeyChanged;

	public TargetOperation(final TargetTable targetTable, final BinlogOperation binlogOperation) {
		this.targetTable = targetTable;
		this.tableName = targetTable.getName();
		this.isGroupKeyChanged = binlogOperation.isGroupKeyChanged();
		this.datMap = targetTable.getTargetDataMap(binlogOperation.getDatMap());
		this.keyMap = targetTable.getTargetDataMap(binlogOperation.getKeyMap());
	}
}