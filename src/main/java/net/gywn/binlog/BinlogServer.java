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

import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.Data;
import lombok.Getter;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogTable;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;

@Data
public class BinlogServer {
	private static final Logger logger = LoggerFactory.getLogger(BinlogServer.class);
	private BinlogHandler binlogHandler;

	private String binlogServer;
	private int binlogServerID;
	private String binlogServerUsername;
	private String binlogServerPassword;
	private String binlogInfoFile;
	private UldraConfig uldraConfig;

	private BinaryLogClient binaryLogClient = null;

	@Getter
	private Map<Long, BinlogTable> binlogTableMap = new HashMap<Long, BinlogTable>();

	@Getter
	private Calendar time = Calendar.getInstance();

	@Getter
	private boolean threadRunning = false;

	@Getter
	private Exception threadException;

	public BinlogServer(final UldraConfig uldraConfig) {
		this.uldraConfig = uldraConfig;
		this.binlogServer = uldraConfig.getBinlogServer();
		this.binlogServerID = uldraConfig.getBinlogServerID();
		this.binlogServerUsername = uldraConfig.getBinlogServerUsername();
		this.binlogServerPassword = uldraConfig.getBinlogServerPassword();
		this.binlogInfoFile = uldraConfig.getBinlogInfoFile();
		this.binlogServerPassword = this.binlogServerPassword == null ? "" : this.binlogServerPassword;
		this.binlogHandler = new BinlogHandler(uldraConfig);
	}

	public void start() {
		try {
			// ============================
			// Initialize binlog client
			// ============================
			String[] binlogServerInfo = binlogServer.split(":");
			String binlogServerUrl = binlogServerInfo[0];
			int binlogServerPort = 3306;
			try {
				binlogServerPort = Integer.parseInt(binlogServerInfo[1]);
			} catch (Exception e) {
				logger.info("Binlog server port not defined, set {}", binlogServerPort);
			}

			binaryLogClient = new BinaryLogClient(binlogServerUrl, binlogServerPort, binlogServerUsername,
					binlogServerPassword);
			EventDeserializer eventDeserializer = new EventDeserializer();
			eventDeserializer.setCompatibilityMode(DATE_AND_TIME_AS_LONG_MICRO, CHAR_AND_BINARY_AS_BYTE_ARRAY);
			binaryLogClient.setEventDeserializer(eventDeserializer);
			binaryLogClient.setServerId(binlogServerID);
			registerEventListener();
			registerLifecycleListener();

			Binlog currentServerBinlog = binlogHandler.getDatabaseBinlog();

			// ============================
			// load binlog position
			// ============================
			Binlog currntBinlog = null, targetBinlog = null;
			try {
				Binlog[] binlogs = Binlog.read(binlogInfoFile);
				currntBinlog = binlogs[0];
				targetBinlog = binlogs[1];
			} catch (Exception e) {
				logger.info("Start binlog position from {}", currentServerBinlog);
				currntBinlog = currentServerBinlog;
			}

			if (targetBinlog == null) {
				logger.info("Start binlog position from {}", currentServerBinlog);
				targetBinlog = currentServerBinlog;
			}

			binlogHandler.setCurrntBinlog(currntBinlog);
			binlogHandler.setTargetBinlog(targetBinlog);

			logger.info("Replicator start from {} to {}", currntBinlog, targetBinlog);
			binaryLogClient.setBinlogFilename(currntBinlog.getBinlogFile());
			binaryLogClient.setBinlogPosition(currntBinlog.getBinlogPosition());

			// ========================================
			// binlog flush (every 500ms) & monitoring
			// ========================================
			new Thread(new Runnable() {

				public void run() {
					while (true) {
						try {
							int currentJobCount = binlogHandler.getJobCount();
							List<Binlog> binlogList = binlogHandler.getWorkerBinlogList();

							Binlog binlog = null, lastBinlog = null;
							if (binlogList.size() > 0) {
								Binlog[] binlogArray = new Binlog[binlogList.size()];
								binlogList.toArray(binlogArray);
								Arrays.sort(binlogArray);
								binlog = currentJobCount > 0 ? binlogArray[0] : binlogArray[binlogArray.length - 1];
								lastBinlog = binlogHandler.getCurrntBinlog();
							}

							if (binlog == null) {
								binlog = binlogHandler.getCurrntBinlog();
								lastBinlog = binlogHandler.getTargetBinlog();
							}

							if (binlogHandler.isRecovering() && !binlogHandler.isRecoveringPosition()) {
								logger.info("Recover finished, target - {}", binlogHandler.getTargetBinlog());
								binlogHandler.setRecovering(false);
							}

							// flush binlog position info
							Binlog.flush(binlog, lastBinlog, binlogInfoFile);

						} catch (Exception e) {
							logger.debug("Flush failed - ", e);

						} finally {
							UldraUtil.sleep(500);
						}

					}
				}

			}, "uldra").start();
			binaryLogClient.connect();

		} catch (Exception e) {
			threadException = e;
			threadRunning = false;
			logger.error(e.toString());
		}

	}

	private void registerEventListener() {
		binaryLogClient.registerEventListener(new EventListener() {
			public void onEvent(Event event) {
				BinlogEvent.valuOf(event.getHeader().getEventType()).receiveEvent(event, binlogHandler);
			}
		});
	}

	private void registerLifecycleListener() {
		binaryLogClient.registerLifecycleListener(new LifecycleListener() {

			public void onConnect(BinaryLogClient client) {
				logger.info("mysql-binlog-connector start from {}:{}", client.getBinlogFilename(),
						client.getBinlogPosition());
				threadRunning = true;
			}

			public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				logger.error("CommunicationFailure - {}", ex.getMessage());
				threadRunning = false;
			}

			public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				logger.error("EventDeserializationFailur - {}", ex.getMessage());
				threadRunning = false;
			}

			public void onDisconnect(BinaryLogClient client) {
				logger.info("Disconnect");
				threadRunning = false;
			}
		});

	}

	public enum BinlogEvent {

		WRITE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveWriteRowsEvent(event);
			}
		},
		UPDATE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveUpdateRowsEvent(event);
			}
		},
		DELETE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveDeleteRowsEvent(event);
			}
		},
		TABLE_MAP {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveTableMapEvent(event);
			}
		},
		ROTATE {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveRotateEvent(event);
			}
		},
		QUERY {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveQueryEvent(event);
			}
		},
		XID {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
				binlogHandler.receiveXidEvent(event);
			}
		},
		NOOP {
			@Override
			public void receiveEvent(final Event event, final BinlogHandler binlogHandler) {
			}
		};

		private static final Map<EventType, BinlogEvent> map = new HashMap<EventType, BinlogEvent>();
		static {
			// ==========================
			// Initialize
			// ==========================
			for (EventType e : EventType.values()) {
				map.put(e, NOOP);
			}

			// ==========================
			// Set Event Type Map
			// ==========================
			map.put(EventType.PRE_GA_WRITE_ROWS, WRITE_ROWS);
			map.put(EventType.WRITE_ROWS, WRITE_ROWS);
			map.put(EventType.EXT_WRITE_ROWS, WRITE_ROWS);

			map.put(EventType.PRE_GA_UPDATE_ROWS, UPDATE_ROWS);
			map.put(EventType.UPDATE_ROWS, UPDATE_ROWS);
			map.put(EventType.EXT_UPDATE_ROWS, UPDATE_ROWS);

			map.put(EventType.PRE_GA_DELETE_ROWS, DELETE_ROWS);
			map.put(EventType.DELETE_ROWS, DELETE_ROWS);
			map.put(EventType.EXT_DELETE_ROWS, DELETE_ROWS);

			map.put(EventType.TABLE_MAP, TABLE_MAP);
			map.put(EventType.QUERY, QUERY);
			map.put(EventType.ROTATE, ROTATE);
			map.put(EventType.XID, XID);

		}

		public abstract void receiveEvent(final Event event, final BinlogHandler binlogHandler);

		public static BinlogEvent valuOf(EventType eventType) {
			BinlogEvent binlogEvent = map.get(eventType);
			logger.debug("{}->BinlogEvent.{}", eventType, binlogEvent);
			return binlogEvent;
		}
	}
}