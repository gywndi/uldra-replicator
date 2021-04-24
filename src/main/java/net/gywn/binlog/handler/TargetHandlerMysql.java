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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.common.UldraConfig;

public class TargetHandlerMysql implements TargetHandler {
	private static final Logger logger = LoggerFactory.getLogger(TargetHandlerMysql.class);

	@Override
	public void init(final UldraConfig uldraConfig) throws Exception {
		logger.info("UldraMysqlApplier->init()");
	}

	@Override
	public void insert(final Connection connection, final TargetOperation operation) throws Exception {
		logger.debug("insert {}", operation);

		StringBuffer sbCol = new StringBuffer();
		StringBuffer sbVal = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbCol.length() > 0) {
				sbCol.append(",");
				sbVal.append(",");
			}
			sbCol.append(e.getKey());
			sbVal.append("?");
			params.add(e.getValue());
		}

		String sql = String.format("insert%s into %s (%s) values (%s)", getIgnore(operation), operation.getTableName(),
				sbCol.toString(), sbVal.toString());

		executeUpdate(connection, sql, params);
	}

	@Override
	public void upsert(final Connection connection, final TargetOperation operation) throws Exception {
		logger.debug("insert {}", operation);

		StringBuffer sbCol = new StringBuffer();
		StringBuffer sbVal = new StringBuffer();
		StringBuffer sbDup = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbCol.length() > 0) {
				sbCol.append(",");
				sbVal.append(",");
			}
			sbCol.append(e.getKey());
			sbVal.append("?");
			if (!e.getKey().equals(operation.getTargetTable().getGroupKey())) {
				if (sbDup.length() > 0) {
					sbDup.append(",");
				}
				sbDup.append(String.format("%s=values(%s)", e.getKey(), e.getKey()));
			}
			params.add(e.getValue());
		}

		String sql = String.format("insert%s into %s (%s) values (%s) on duplicate key update %s", getIgnore(operation),
				operation.getTableName(), sbCol.toString(), sbVal.toString(), sbDup.toString());
		executeUpdate(connection, sql, params);
	}

	@Override
	public void update(final Connection connection, final TargetOperation operation) throws Exception {
		logger.debug("update {}", operation);

		StringBuffer sbSet = new StringBuffer();
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// set
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbSet.length() > 0) {
				sbSet.append(",");
			}
			sbSet.append(String.format("%s=?", e.getKey()));
			params.add(e.getValue());
		}

		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			sbWhe.append(String.format("and %s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("update%s %s set %s where 1=1 %s", getIgnore(operation), operation.getTableName(),
				sbSet.toString(), sbWhe.toString());
		executeUpdate(connection, sql, params);

	}

	@Override
	public void delete(final Connection connection, final TargetOperation operation) throws Exception {
		logger.debug("delete {}", operation);

		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			sbWhe.append(String.format("and %s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("delete%s from %s where 1=1 %s", getIgnore(operation), operation.getTableName(),
				sbWhe.toString());
		executeUpdate(connection, sql, params);
	}

	@Override
	public void softdel(final Connection connection, final TargetOperation operation) throws Exception {
		logger.debug("softdel {}", operation);

		StringBuffer sbSet = new StringBuffer();
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// set
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbSet.length() > 0) {
				sbSet.append(",");
			}
			sbSet.append(String.format("%s=default(%s)", e.getKey(), e.getKey()));
		}

		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			sbWhe.append(String.format("and %s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("update%s %s set %s where 1=1 %s", getIgnore(operation), operation.getTableName(),
				sbSet.toString(), sbWhe.toString());
		executeUpdate(connection, sql, params);
	}

	@Override
	public void begin(final Connection connection) throws Exception {
		logger.debug("begin");
		connection.setAutoCommit(false);

	}

	@Override
	public void commit(final Connection connection) throws Exception {
		logger.debug("commit");
		connection.commit();
	}

	@Override
	public void rollback(final Connection connection) throws Exception {
		logger.debug("rollback");
		connection.rollback();
	}

	@Override
	public Map<String, String> selectByOld(Connection connection, TargetOperation operation) throws Exception {

		StringBuffer sbCol = new StringBuffer();
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql

		// columns
		for (Entry<String, String> e : operation.getTargetTable().getColumnMapper().entrySet()) {
			if (sbCol.length() > 0) {
				sbCol.append(",");
			}
			sbCol.append(e.getValue());
		}

		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			sbWhe.append(String.format("and %s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("select %s from %s where 1=1 %s", sbCol.toString(), operation.getTableName(),
				sbWhe.toString());

		PreparedStatement pstmt = connection.prepareStatement(sql);
		int seq = 1;
		for (String param : params) {
			pstmt.setString(seq++, param);
		}

		Map<String, String> map = new HashMap<String, String>();
		ResultSet rs = pstmt.executeQuery();
		ResultSetMetaData rsMeta = rs.getMetaData();
		if (rs.next()) {
			for (int i = 0; i < rsMeta.getColumnCount(); i++) {
				String key = rsMeta.getColumnLabel(i + 1).toLowerCase();
				String val = rs.getString(i + 1);
				map.put(key, val);
			}
			rs.close();
			return map;
		}
		return null;
	}

	private static void executeUpdate(final Connection connection, final String sql, final List<String> params)
			throws Exception {
		logger.debug("{}, {}", sql, params);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		int seq = 1;
		for (String param : params) {
			pstmt.setString(seq++, param);
		}
		pstmt.executeUpdate();
		pstmt.close();
	}

	private String getIgnore(final TargetOperation operation) {
		return operation.isRecovering() ? " ignore" : "";
	}

}
