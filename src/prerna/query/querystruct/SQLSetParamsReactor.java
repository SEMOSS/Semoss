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
package prerna.query.querystruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SQLSetParamsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SQLSetParamsReactor.class);

	public SQLSetParamsReactor() {
		// id _type can be column, column_table, colum_table_operator
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.ID_TYPE.getKey() };
	}

	// execute method - GREEDY translation
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String id = keyValue.get(keysToGet[0]);
		Object value = keyValue.get(keysToGet[1]);

		String type = "column";
		if (keyValue.containsKey(keysToGet[2])) {
			type = keyValue.get(keysToGet[2]);
		}

		Object obj = insight.getVar(SQLGetParamsReactor.QS_WRAPPER);
		String query = "No such id found";
		ITableDataFrame frame = insight.getCurFrame();
		SelectQueryStruct sqs = null;
		if (frame != null && frame instanceof NativeFrame) {
			sqs = ((NativeFrame) frame).getQueryStruct(); // this.insight.getLastQS(insight.getLastPanelId());
		}

		if (obj == null && sqs != null) {
			// may be the user is doing for first time create it
			String curQuery = sqs.getCustomFrom();

			SqlParser sqp2 = new SqlParser();
			try {
				GenExpressionWrapper wrapper = sqp2.processQuery(curQuery);
				Object[] allColumns = wrapper.columnTableIndex.keySet().toArray();
				insight.getVarStore().put(SQLGetParamsReactor.QS_WRAPPER,
						new NounMetadata(wrapper, PixelDataType.CUSTOM_DATA_STRUCTURE));
				obj = wrapper;
			} catch (Exception e) {
				classLogger.error("Failed to parse the SQL query {}", curQuery, e);
			}
		}

		if (obj != null) {
			GenExpressionWrapper wrapper = (GenExpressionWrapper) obj;

			if (type.equalsIgnoreCase("column")) {
				wrapper.replaceColumn(id, value);
			} else if (type.equalsIgnoreCase("column_table")) {
				wrapper.replaceTableColumn(id, value);
			}
			if (type.equalsIgnoreCase("column_table_operator")) {
				wrapper.replaceTableColumnOperator(id, value);
			}
			query = "Parameters have been set";
		}
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}

}