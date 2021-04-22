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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.handler.OperationBinlogHandler;

@Getter
@ToString
public class BinlogOperation {
	private static final Logger logger = LoggerFactory.getLogger(BinlogOperation.class);
	private final BinlogTable binlogTable;
	private final OperationBinlogHandler binlogHandler;
	private final Map<String, String> datMap = new HashMap<String, String>();
	private final Map<String, String> keyMap = new HashMap<String, String>();
	private boolean modified = false;

	@Setter
	private boolean groupKeyChanged = false;

	@Setter
	private long crc32Code = 0;

	public BinlogOperation(final BinlogTable binlogTable, final UldraConfig uldraConfig,
			final OperationBinlogHandler binlogHandler) {
		this.binlogTable = binlogTable;
		this.binlogHandler = binlogHandler;
	}

	public void modify() {
		if (!modified) {
			logger.debug("modify->before {}", this);
			binlogTable.getRowHandler().modify(this);
			logger.debug("modify->after  {}", this);
		}
		modified = true;
	}
}
