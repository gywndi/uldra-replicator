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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;
import net.gywn.binlog.handler.TargetHandler;

public class BinlogHandlerWorker {
	private static final Logger logger = LoggerFactory.getLogger(BinlogHandlerWorker.class);
	private final String workerName;
	private final BlockingQueue<BinlogTransaction> queue;
	private final TargetHandler targetHandler;
	private final DataSource targetDataSource;
	private boolean processing = false;
	private Thread thread;

	@Getter
	private Binlog lastExecutedBinlog;

	public BinlogHandlerWorker(final int threadNumber, final UldraConfig uldraConfig) throws SQLException {
		this.workerName = String.format("w%03d", threadNumber);
		this.queue = new ArrayBlockingQueue<BinlogTransaction>(uldraConfig.getWokerQueueSize());
		this.targetHandler = uldraConfig.getTargetHandler();
		this.targetDataSource = uldraConfig.getTargetDataSource();
	}

	public void enqueue(final BinlogTransaction tx) {
		logger.debug("{}->enqueue()", workerName);
		while (true) {
			try {
				queue.add(tx);
				break;
			} catch (Exception e) {
				// TODO: add metric to check enqueue error
				UldraUtil.sleep(100);
			}
		}
	}

	public void start() {
		thread = new Thread(() -> {
			while (true) {

				try {
					// ========================
					// dequeue
					// ========================
					BinlogTransaction binlogTransaction = queue.take();
					logger.debug("dequeue - {}", binlogTransaction);

					// ========================
					// transaction processing
					// ========================
					processing = true;
					while (true) {
						try {
							// begin
							transactionStart(binlogTransaction);

							// processing
							for (final BinlogOperation binlogOperation : binlogTransaction.getBinlogOperations()) {
								binlogOperation.modify();

								// apply to target table
								binlogOperation.getBinlogHandler().executeBinlogOperation(binlogTransaction,
										binlogOperation, targetHandler);
							}

							// commit
							transactionCommit(binlogTransaction);
							lastExecutedBinlog = new Binlog(binlogTransaction.getPosition());
							break;
						} catch (Exception e) {
							logger.error(e.getMessage());
							e.printStackTrace();
							transactionRollback(binlogTransaction);
							UldraUtil.sleep(1000);
						}
					}
					processing = false;
				} catch (InterruptedException e) {
					System.out.println("[dequeue]" + e);
				}
			}
		}, workerName);
		thread.start();
		logger.info("{} started", workerName);

	}

	private void transactionStart(BinlogTransaction tx) throws Exception {
		logger.debug("transactionStart()");
		tx.setConnection(targetDataSource.getConnection());
		if (tx.isTransactional()) {
			targetHandler.begin(tx.getConnection());
		}
	}

	private void transactionCommit(BinlogTransaction tx) throws Exception {
		logger.debug("transactionCommit()");
		try {
			if (tx.isTransactional()) {
				targetHandler.commit(tx.getConnection());
			}
		} finally {
			tx.close();
		}
	}

	private void transactionRollback(BinlogTransaction tx) {
		logger.debug("transactionRollback()");
		try {
			if (tx.isTransactional()) {
				targetHandler.rollback(tx.getConnection());
			}
		} catch (Exception e) {
			logger.error("Rollback error", e.getMessage());
		} finally {
			tx.close();
		}
	}

	// Current processing job + jobs in queue
	public int getJobCount() {
		return queue.size() + (processing ? 1 : 0);
	}

}
