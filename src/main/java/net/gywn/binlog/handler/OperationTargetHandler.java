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

package net.gywn.binlog.handler;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.beans.TargetOperation;

public enum OperationTargetHandler {
	INSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			if (targetOperation.isRecovering()) {
				logger.debug("TargetOpType->INSERT->UPSERT");
				targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
				return;
			}

			logger.debug("TargetOpType->INSERT");
			targetHandler.insert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPDATE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			// check key map if empty
			if (targetOperation.getKeyMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_ROWKEY {}", targetOperation);
				return;
			}

			if (targetOperation.isGroupKeyChanged()) {
				logger.debug("TargetOpType->UPDATE->KEY_CHANGED");

				// Fill by old image
				Map<String, String> map = targetHandler.selectByOld(binlogTransaction.getConnection(), targetOperation);

				// no data, do nothing
				if (map == null) {
					return;
				}
				map.remove(targetOperation.getTargetTable().getGroupKey());

				// merge data
				for (Entry<String, String> entry : map.entrySet()) {
					if (!targetOperation.getDatMap().containsKey(entry.getKey())) {
						targetOperation.getDatMap().put(entry.getKey(), entry.getValue());
					}
				}

				logger.debug("TargetOpType->INSERT->NO_ROWKEY->MERGED {}", targetOperation);
				targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
				targetHandler.delete(binlogTransaction.getConnection(), targetOperation);
				return;
			}

			logger.debug("TargetOpType->UPDATE");
			targetHandler.update(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			logger.debug("TargetOpType->UPSERT");
			targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check key map if empty
			if (targetOperation.getKeyMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_ROWKEY {}", targetOperation);
				return;
			}

			logger.debug("TargetOpType->DELETE");
			targetHandler.delete(binlogTransaction.getConnection(), targetOperation);
		}
	},
	SOFT_DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {
			logger.debug("TargetOpType->SOFT_DELETE");
			Map<String, String> keyMap = targetOperation.getKeyMap();
			Map<String, String> datMap = targetOperation.getDatMap();
			for (Entry<String, String> entry : targetOperation.getTargetTable().getColumnMapper().entrySet()) {
				String column = entry.getValue();
				if (keyMap.containsKey(column)) {
					continue;
				}
				datMap.put(column, null);
			}
			targetHandler.softdel(binlogTransaction.getConnection(), targetOperation);
		}
	};

	private static final Logger logger = LoggerFactory.getLogger(OperationTargetHandler.class);

	public abstract void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
			final TargetHandler targetHandler) throws Exception;

}
