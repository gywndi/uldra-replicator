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

package net.gywn.binlog.custom;

import net.gywn.binlog.handler.RowHandler;
import net.gywn.binlog.beans.BinlogOperation;

public class RowHandlerCustom implements RowHandler {

	@Override
	public void init() {
		// TODO Auto-generated method stub
	}

	@Override
	public void modify(final BinlogOperation operation) {
		String nameOrigin = operation.getDatMap().get("name");
		String addrOrigin = operation.getDatMap().get("addr");
		String newname = String.format("%s:%s", nameOrigin, addrOrigin);
		operation.getDatMap().put("newname", newname);
	}
}
