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

import java.sql.Connection;
import java.util.Map;

import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.common.UldraConfig;

public interface TargetHandler {

	public void init(final UldraConfig uldraConfig) throws Exception;

	public void begin(final Connection connection) throws Exception;

	public void commit(final Connection connection) throws Exception;

	public void rollback(final Connection connection) throws Exception;

	public void insert(final Connection connection, final TargetOperation operation) throws Exception;

	public void upsert(final Connection connection, final TargetOperation operation) throws Exception;

	public void update(final Connection connection, final TargetOperation operation) throws Exception;

	public void delete(final Connection connection, final TargetOperation operation) throws Exception;

	public void softdel(final Connection connection, final TargetOperation operation) throws Exception;
	
	public Map<String, String> selectByOld(final Connection connection, final TargetOperation operation) throws Exception;

}
