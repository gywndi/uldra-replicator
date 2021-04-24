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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.beans.TargetTable;

public enum OperationBinlogHandler {
	INS {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation, binlogTransaction.isRecovering());
				targetTable.getInsert().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}

	},
	UPD {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation, binlogTransaction.isRecovering());
				targetTable.getUpdate().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}
	},
	DEL {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation, binlogTransaction.isRecovering());
				targetTable.getDelete().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}
	},
	NON {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
		}
	};

	private static final Logger logger = LoggerFactory.getLogger(OperationBinlogHandler.class);

	public abstract void executeBinlogOperation(final BinlogTransaction binlogTransaction,
			final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception;

}
