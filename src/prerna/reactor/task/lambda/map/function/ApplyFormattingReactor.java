/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.task.lambda.map.function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class ApplyFormattingReactor extends TaskBuilderReactor {

	private Map<String, String> colIndexFormatMap;
	private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private final DateFormat stf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public ApplyFormattingReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	protected void buildTask() {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
			ITableDataFrame frame = qs.getFrame();
			Map<String, String> additionalDataTypeMap = frame.getMetaData().getHeaderToAdtlTypeMap();

			if (!additionalDataTypeMap.isEmpty()) {
				// populate the colIndexFormatMap, which will indicate the column index and the
				// format requested for a column
				Map<String, SemossDataType> dataTypeMap = frame.getMetaData().getHeaderToTypeMap();
				List<IQuerySelector> selectors = qs.getSelectors();
				populateColIndexFormatMap(selectors, additionalDataTypeMap, dataTypeMap, frame.getName());

				// create a new iterator task and attach it to the previous iterator
				ApplyFormattingTask newTask = new ApplyFormattingTask();
				newTask.setInnerTask(this.task);
				newTask.setApplyFormattingReactor(this);
				this.task = newTask;
				this.insight.getTaskStore().addTask(this.task);
			}
		}
	}

	private void populateColIndexFormatMap(List<IQuerySelector> selectors, Map<String, String> additionalDataTypeMap,
			Map<String, SemossDataType> dataTypeMap, String frame) {
		Map<String, String> map = new HashMap<String, String>();
		for (IQuerySelector s : selectors) {
			String col = s.getQueryStructName();
			if (!col.contains("__")) {
				col = frame + "__" + col;
			}
			if (additionalDataTypeMap.containsKey(col)) {
				int indx = selectors.indexOf(s);
				String type = dataTypeMap.get(col).toString();
				String format = additionalDataTypeMap.get(col);
				map.put(indx + "|" + type, format);
			}
		}
		this.colIndexFormatMap = map;
	}

	public IHeadersDataRow process(IHeadersDataRow row) {
		String[] headers = row.getHeaders();
		Object[] values = row.getValues();
		for (String key : this.colIndexFormatMap.keySet()) {
			int pos = Integer.parseInt(key.split("\\|")[0]);
			String type = key.split("\\|")[1];
			String val = values[pos].toString();

			try {
				// convert the value to a java Date
				Date valAsDateTime = null;
				if (type.equals("DATE")) {
					valAsDateTime = sdf.parse(val);
				} else if (type.equals("TIMESTAMP")) {
					valAsDateTime = stf.parse(val);
				}
				// format the date per requested format
				DateFormat df = new SimpleDateFormat(this.colIndexFormatMap.get(key));
				values[pos] = df.format(valAsDateTime);
			} catch (ParseException e1) {
				values[pos] = null;
			}
		}
		return new HeadersDataRow(headers, values);
	}
}

/**
 * Inner class to apply the formatting
 */
class ApplyFormattingTask extends AbstractTaskOperation {

	private ApplyFormattingReactor applyFormattingReactor;

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow row = this.innerTask.next();
		return applyFormattingReactor.process(row);
	}

	public void setApplyFormattingReactor(ApplyFormattingReactor applyFormattingReactor) {
		this.applyFormattingReactor = applyFormattingReactor;
	}
}
