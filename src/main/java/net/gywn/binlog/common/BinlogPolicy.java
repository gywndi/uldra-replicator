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

import java.util.HashMap;
import java.util.Map;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.handler.RowHandler;

@Setter
@Getter
@ToString
public class BinlogPolicy {
	private String name;
	private String groupKey;
	private String preciseAlgorithmClassName = "net.gywn.binlog.shardingsphere.PreciseShardingCRC32";
	private PreciseShardingAlgorithm<?> preciseShardingAlgorithm;

	private String rowHandlerClassName = "net.gywn.binlog.handler.RowHandlerDefault";;
	private Map<String, String> rowHandlerParams = new HashMap<String, String>();
	private RowHandler rowHandler;
	private ReplicatPolicy[] replicatPolicies;

	public void setName(String name) {
		this.name = name.toLowerCase();
	}

	public void loadHandlers() throws Exception {
		this.preciseShardingAlgorithm = (PreciseShardingAlgorithm<?>) (Class.forName(preciseAlgorithmClassName))
				.newInstance();
		this.rowHandler = (RowHandler) (Class.forName(rowHandlerClassName)).newInstance();
		this.rowHandler.init();
	}
}
