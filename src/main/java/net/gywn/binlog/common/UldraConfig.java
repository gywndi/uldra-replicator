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

package net.gywn.binlog.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.handler.TargetHandler;
import net.gywn.binlog.beans.BinlogTable;

@Setter
@Getter
@ToString
public class UldraConfig {
	private static final Logger logger = LoggerFactory.getLogger(UldraConfig.class);

	private int workerCount = 4;
	private int wokerQueueSize = 500;
	private String binlogServer = "127.0.0.1:3306";
	private int binlogServerID = 5;
	private String binlogServerUsername = "repl";
	private String binlogServerPassword = "repl";
	private String binlogInfoFile;
	private String targetHandlerClassName = "net.gywn.binlog.handler.TargetHandlerMysql";
	private String targetHandlerParamFile;
	private BinlogPolicy[] binlogPolicies;

	private DataSource binlogDataSource;
	private DataSource targetDataSource;
	private DataSource[] dataSources;
	private TargetHandler targetHandler;
	private CaseInsensitiveMap<String, BinlogTable> tableMap = new CaseInsensitiveMap<String, BinlogTable>();
	private CaseInsensitiveMap<String, BinlogPolicy> binlogPolicyMap = new CaseInsensitiveMap<String, BinlogPolicy>();
	private CaseInsensitiveMap<String, ReplicatPolicy> replicatPolicyMap = new CaseInsensitiveMap<String, ReplicatPolicy>();

	public static UldraConfig loadUldraConfig(final String uldraConfigFile) throws Exception {
		try (FileInputStream fileInputStream = new FileInputStream(new File(uldraConfigFile));
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")) {
			UldraConfig uldraConfig = new Yaml(new Constructor(UldraConfig.class)).loadAs(inputStreamReader,
					UldraConfig.class);
			return uldraConfig;
		}
	}

	public void modifyBinlogFile(final String binlogInfo) {
		String fileInfo = null;
		try {
			fileInfo = new String(Files.readAllBytes(Paths.get(binlogInfoFile)), StandardCharsets.UTF_8);
			logger.info("Current binlog file info {}", fileInfo);
		} catch (IOException e) {
			logger.error("Read on {} fail - ", binlogInfoFile, e.getMessage());
		}

		try {
			Files.write(Paths.get(binlogInfoFile), binlogInfo.replaceAll("\\s", "").getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			logger.error("Write on {} fail - ", binlogInfoFile, e.getMessage());
		}
	}

	public void init() throws Exception {
		// ===================================
		// DataSource initialize
		// ===================================
		Class.forName("com.mysql.jdbc.Driver");
		BasicDataSource tempDataSource = new BasicDataSource();
		String jdbcUrl = String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false&connectTimeout=3000",
				this.binlogServer, "information_schema");
		tempDataSource.setUrl(jdbcUrl);
		tempDataSource.setUsername(binlogServerUsername);
		tempDataSource.setPassword(binlogServerPassword);
		tempDataSource.setDefaultAutoCommit(true);
		tempDataSource.setInitialSize(5);
		tempDataSource.setMinIdle(5);
		tempDataSource.setMaxIdle(5);
		tempDataSource.setMaxTotal(10);
		tempDataSource.setMaxWaitMillis(1000);
		tempDataSource.setTestOnBorrow(false);
		tempDataSource.setTestOnReturn(false);
		tempDataSource.setTestWhileIdle(true);
		tempDataSource.setNumTestsPerEvictionRun(3);
		tempDataSource.setTimeBetweenEvictionRunsMillis(60000);
		tempDataSource.setMinEvictableIdleTimeMillis(600000);
		tempDataSource.setValidationQuery("SELECT 1");
		tempDataSource.setValidationQueryTimeout(5);
		binlogDataSource = tempDataSource;

		logger.info("==================================");
		logger.info("Binlog policy initializing..");
		logger.info("==================================");

		this.targetDataSource = dataSources[0];

		// ShardingSphere configuration
		ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
		String dataNodes = String.format("ds${0..%d}", dataSources.length - 1);

		Map<String, Integer> mergeTargetTables = new HashMap<String, Integer>();
		for (BinlogPolicy binlogPolicy : binlogPolicies) {
			logger.info("[{}] Initializing..", binlogPolicy.getName());

			// ============================================
			// Check transaction policies if it duplicated
			// ============================================
			if (binlogPolicyMap.containsKey(binlogPolicy.getName())) {
				throw new Exception("Duplicate binlog policy name" + binlogPolicy.getName() + ", exit");
			}
			binlogPolicyMap.put(binlogPolicy.getName(), binlogPolicy);

			// ============================================
			// Load handlers for binlog policies
			// ============================================
			logger.info("- Load rowHandler {}", binlogPolicy.getName());
			binlogPolicy.loadHandlers();

			// ============================================
			// Check target policies
			// ============================================
			if (binlogPolicy.getReplicatPolicies() == null) {
				logger.info("- NO replicate policy");
				binlogPolicy.setReplicatPolicies(new ReplicatPolicy[] { new ReplicatPolicy() });
			}

			for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
				String targetName = replicatPolicy.getName();
				if (targetName == null) {
					logger.info("- Target name is null, get name from source table");
					targetName = binlogPolicy.getName().trim().replaceAll("(.*)\\.(.*)$", "$2");
				}
				targetName = targetName.toLowerCase();
				logger.info("- Set target table name {}", targetName);
				replicatPolicy.setName(targetName);

				// increase target merged table
				Integer mergeTableTableCount = mergeTargetTables.get(targetName);
				if (mergeTableTableCount == null) {
					mergeTargetTables.put(targetName, 1);
				} else {
					mergeTargetTables.put(targetName, mergeTableTableCount + 1);
				}
				System.out.println();

				// ShardingSphere
				if (dataSources.length > 1) {
					// sharding target
					TableRuleConfiguration tableRule = new TableRuleConfiguration(replicatPolicy.getName(),
							String.format("%s.%s", dataNodes, replicatPolicy.getName()));

					// sharding rules
					if (binlogPolicy.getGroupKey() != null) {
						tableRule.setDatabaseShardingStrategyConfig(new StandardShardingStrategyConfiguration(
								binlogPolicy.getGroupKey(), binlogPolicy.getPreciseShardingAlgorithm()));
						shardingRuleConfig.getTableRuleConfigs().add(tableRule);
						shardingRuleConfig.getBindingTableGroups().add(replicatPolicy.getName());
						continue;
					}

					// set broadcast table if no group key
					shardingRuleConfig.getBroadcastTables().add(replicatPolicy.getName());

				}
			}
		}

		for (Entry<String, Integer> entry : mergeTargetTables.entrySet()) {
			if (entry.getValue() > 1) {
				String refStr = "";
				for (BinlogPolicy binlogPolicy : binlogPolicies) {
					for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
						replicatPolicy.setSoftDelete(true);
						replicatPolicy.setUpsertMode(true);
						refStr += binlogPolicy.getName() + " ";
					}
					logger.info("{} referenced by {}, set softDelete and upsert", entry.getKey(), binlogPolicy.getName());
				}
			}
		}

		// ShardingSphere dbcp
		if (dataSources.length > 1) {
			Map<String, DataSource> nodeMap = new HashMap<>();
			for (int i = 0; i < dataSources.length; i++) {
				nodeMap.put(String.format("ds%d", i), dataSources[i]);
			}
			this.targetDataSource = ShardingDataSourceFactory.createDataSource(nodeMap, shardingRuleConfig, null);
		}

		logger.info("==================================");
		logger.info("Load target handler start");
		logger.info("==================================");
		try {
			this.targetHandler = (TargetHandler) (Class.forName(targetHandlerClassName)).newInstance();
			this.targetHandler.init(this);
			logger.info("{} loaded", targetHandlerClassName);
		} catch (Exception e) {
			logger.error("Load target handler fail - ", e.getMessage());
		}

		logger.info("==================================");
		logger.info("Define binlog info file");
		logger.info("==================================");
		if (this.binlogInfoFile == null) {
			logger.info("Binlog info file is not defined, set binlog-pos-{}:{}.info", binlogServer, binlogServerID);
			this.binlogInfoFile = String.format("binlog-pos-%s:%d.info", binlogServer, binlogServerID);
		}
	}
}