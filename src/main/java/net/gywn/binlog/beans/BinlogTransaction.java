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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
public class BinlogTransaction implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(BinlogTransaction.class);
	
	private final List<BinlogOperation> binlogOperations = new ArrayList<BinlogOperation>();
	private final String position;
	private final Binlog binlog;
	private boolean recovering = false;
	
	// TODO: set default transactional to true if target is not MySQL
	private boolean transactional = false;

	@Setter
	private Connection connection;

	public BinlogTransaction(final String position, final boolean recovering) {
		this.position = position;
		this.recovering = recovering;
		this.binlog = new Binlog(position);
	}

	public void addOperation(final BinlogOperation binlogOperation) {
		logger.debug("Add operation - {}", binlogOperation);
		if (!transactional && binlogOperation.getBinlogTable().getTargetTables().size() > 1) {
			logger.debug("Set transactional");
			transactional = true;
		}
		binlogOperations.add(binlogOperation);
	}

	public int size() {
		return binlogOperations.size();
	}

	public boolean isTransactional() {
		if (transactional || binlogOperations.size() > 1) {
			return true;
		}
		return false;
	}

	public void close() {
		try {
			connection.close();
		} catch (Exception e) {
		}
	}
}
