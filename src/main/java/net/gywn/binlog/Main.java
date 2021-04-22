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

import java.util.concurrent.Callable;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.common.UldraConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	static {
		try {
			String loggingConfigFile = System.getProperty("java.util.logging.config.file");
			if (loggingConfigFile == null) {
				loggingConfigFile = "log4j.properties";
			}
			PropertyConfigurator.configure(loggingConfigFile);
		} catch (Exception e) {
		}
	}

	@Option(names = { "--config-file" }, description = "Config file", required = true)
	private String configFile;

	@Option(names = { "--worker-count" }, description = "Worker count", required = false)
	private Integer workerCount;

	@Option(names = { "--worker-queue-size" }, description = "Worker queue count", required = false)
	private Integer workerQueueSize;

	@Option(names = { "--binlog-info" }, description = "Binlog position info", required = false)
	private String binlogInfo;

	public static void main(String[] args) {

		Main main = new Main();
		if (args.length == 0) {
			args = new String[] { "--config-file", "uldra-config.yml" };
		}
		Integer exitCode = new CommandLine(main).execute(args);

		if (exitCode != 0) {
			logger.error("exit code: {}", exitCode);
		}
	}

	@Override
	public Integer call() {

		try {
			logger.info("Load from {}", configFile);
			UldraConfig uldraConfig = UldraConfig.loadUldraConfig(configFile);
			if (workerCount != null) {
				uldraConfig.setWorkerCount(workerCount);
			}
			if (workerQueueSize != null) {
				uldraConfig.setWorkerCount(workerQueueSize);
			}
			if (binlogInfo != null) {
				uldraConfig.modifyBinlogFile(binlogInfo);
			}
			uldraConfig.init();

			logger.info(uldraConfig.toString());
			BinlogServer binlogServer = new BinlogServer(uldraConfig);
			binlogServer.start();

		} catch (Exception e) {
			logger.error(e.getMessage());
			return 1;
		}
		return 0;
	}
}