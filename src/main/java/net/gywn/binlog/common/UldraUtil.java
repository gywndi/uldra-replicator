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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.beans.BinlogColumn;

public class UldraUtil {
	private static final Logger logger = LoggerFactory.getLogger(UldraUtil.class);
	private static final CaseInsensitiveMap<String, String> charMap = new CaseInsensitiveMap<String, String>();
	static {
		charMap.put("euckr", "MS949");
		charMap.put("utf8", "UTF-8");
		charMap.put("utf8mb4", "UTF-8");
	}

	public static long crc32(final String s) {
		Checksum checksum = new CRC32();
		long num = 0;
		try {
			byte[] bytes = s.getBytes();
			checksum.update(bytes, 0, bytes.length);
			num = checksum.getValue();
		} catch (Exception e) {
			logger.error("crc32 error {}", e.getMessage());
		}
		return num;
	}

	public static void sleep(final long sleepMili) {
		try {
			Thread.sleep(sleepMili);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
		}
	}

	public static void writeFile(final String filename, final String info) {
		logger.debug("writeFile {} - {}", filename, info);
		try {
			Files.write(Paths.get(filename), info.getBytes(StandardCharsets.UTF_8));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public static String readFile(String path) {
		logger.debug("readFile {}", path);
		try {
			byte[] encoded = Files.readAllBytes(Paths.get(path));
			return new String(encoded, StandardCharsets.UTF_8);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return null;
	}

	private static String getMysqlDatetime(final Serializable serializable) {
		logger.debug("getMysqlDatetime {}", serializable);
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	private static String getMysqlTimestamp(final Serializable serializable) {
		logger.debug("getMysqlTimestamp {}", serializable);
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	private static String getMysqlDate(final Serializable serializable) {
		logger.debug("getMysqlDate {}", serializable);
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s", format.format(new Date(time / 1000)));
	}

	private static String getMysqlTime(final Serializable serializable) {
		logger.debug("getMysqlTime {}", serializable);
		long time = (long) serializable;
		SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("%s.%06d", format.format(new Date(time / 1000)), time % 1000000);
	}

	public static String toString(final Serializable serializable, final BinlogColumn column) {
		if (serializable == null) {
			return null;
		}

		logger.debug("column type in mysql {}", column.getType());
		switch (column.getType()) {
		case "datetime":
			return getMysqlDatetime(serializable);
		case "timestamp":
			return getMysqlTimestamp(serializable);
		case "date":
			return getMysqlDate(serializable);
		case "time":
			return getMysqlTime(serializable);
		}

		if (serializable instanceof String) {
			logger.debug("java String type");
			return (String) serializable;
		}

		if (serializable instanceof java.lang.Integer) {
			logger.debug("java Integer type");
			return column.isUnsigned() ? Integer.toUnsignedString((Integer) serializable) : serializable.toString();
		}

		if (serializable instanceof java.lang.Long) {
			logger.debug("java Long type, unsinged {}", column.isUnsigned());
			return column.isUnsigned() ? Long.toUnsignedString((Long) serializable) : serializable.toString();
		}

		if (serializable instanceof byte[] && column.getCharset() != null) {
			logger.debug("java Bytes type");
			return toCharsetString((byte[]) serializable, column.getCharset());
		}

		logger.debug("java {} type", serializable.getClass());
		return serializable.toString();
	}

	public static String toCharsetString(final byte[] byteArray, final String mysqlCharset) {
		logger.debug("toCharsetString->{}", mysqlCharset);
		String javaCharset = charMap.get(mysqlCharset);
		if (javaCharset != null) {
			try {
				return new String(byteArray, javaCharset);
			} catch (UnsupportedEncodingException e) {
				logger.error(e.getMessage());
			}
		}
		return new String(byteArray);
	}
}
