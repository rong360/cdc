/*
 * Copyright 2013 Stanley Shyiko
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
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.rong360.binlogutil.GlobalConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class UpdateRowsEventDataDeserializer extends AbstractRowsEventDataDeserializer<UpdateRowsEventData> {

	private boolean mayContainExtraInformation;

	public UpdateRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
		super(tableMapEventByTableId);
	}

	public UpdateRowsEventDataDeserializer setMayContainExtraInformation(boolean mayContainExtraInformation) {
		this.mayContainExtraInformation = mayContainExtraInformation;
		return this;
	}

	@Override
	public UpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
		UpdateRowsEventData eventData = new UpdateRowsEventData();
		eventData.setTableId(inputStream.readLong(6));
		if(!GlobalConfig.filterByTableId(eventData.getTableId())){
			return null;
		}
		inputStream.skip(2); // reserved
		if (mayContainExtraInformation) {
			int extraInfoLength = inputStream.readInteger(2);
			inputStream.skip(extraInfoLength - 2);
		}
		int numberOfColumns = inputStream.readPackedInteger();
		eventData.setIncludedColumnsBeforeUpdate(inputStream.readBitSet(numberOfColumns, true));
		eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
		eventData.setRows(deserializeRows(eventData, inputStream));
		return eventData;
	}

	private List<Map.Entry<Serializable[], Serializable[]>> deserializeRows(UpdateRowsEventData eventData,
			ByteArrayInputStream inputStream) throws IOException {
		TableMapEventData tableMapEvent = getTableMapEventData(eventData.getTableId());
		ColumnSet includedColumnsBeforeUpdate = new ColumnSet(eventData.getIncludedColumnsBeforeUpdate()),
				includedColumns = new ColumnSet(eventData.getIncludedColumns());
		List<Map.Entry<Serializable[], Serializable[]>> rows =
				new LinkedList<Map.Entry<Serializable[], Serializable[]>>();
		while (inputStream.available() > 0) {
			rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(
					deserializeRow(tableMapEvent, includedColumnsBeforeUpdate, inputStream),
					deserializeRow(tableMapEvent, includedColumns, inputStream)
					));
		}
		return rows;
	}

}
