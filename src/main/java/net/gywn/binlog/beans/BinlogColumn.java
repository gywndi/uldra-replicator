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

package net.gywn.binlog.beans;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
public class BinlogColumn {
	private final String name;
	private final String charset;
	private final String type;
	private final boolean unsigned;
	
	@Setter
	private boolean rowKey = false;

	// TODO: add default values
	public BinlogColumn(final String name, final String type, final String charset, final boolean unsigned) {
		this.name = name;
		this.charset = charset;
		this.type = type;
		this.unsigned = unsigned;
	}
}
