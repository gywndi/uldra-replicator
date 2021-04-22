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

package net.gywn.binlog.shardingsphere;

import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

public class PreciseShardingCRC32 implements PreciseShardingAlgorithm<Comparable<?>> {

	public String doSharding(Collection<String> availableTargetNames,
			PreciseShardingValue<Comparable<?>> shardingValue) {
		ArrayList<String> list = new ArrayList<String>(availableTargetNames);
		Checksum checksum = new CRC32();
		try {
			byte[] bytes = shardingValue.getValue().toString().getBytes();
			checksum.update(bytes, 0, bytes.length);
			int seq = (int) (checksum.getValue() % list.size());
			return list.get(seq);
		} catch (Exception e) {
		}
		return list.get(0);
	}
}