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
package prerna.rdf.engine.wrappers;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class RawImpalaSelectWrapper extends RawRDBMSSelectWrapper {

	private static final Logger classLogger = LogManager.getLogger(RawImpalaSelectWrapper.class);

	private SelectQueryStruct qs;

	public RawImpalaSelectWrapper() {

	}

	public RawImpalaSelectWrapper(SelectQueryStruct qs) {
		this.qs = qs;
	}

	@Override
	protected void setVariables() {
		try {
			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			colTypes = new int[numColumns];
			types = new SemossDataType[numColumns];
			rawHeaders = new String[numColumns];
			headers = new String[numColumns];

			for (int colIndex = 1; colIndex <= numColumns; colIndex++) {
				rawHeaders[colIndex - 1] = rsmd.getColumnName(colIndex);
				headers[colIndex - 1] = rsmd.getColumnLabel(colIndex);
				// IMPALA EDITS
				// Remove the front appended math function and re-add it to address case issue
				// due to impala returning lowercase only
				if (qs != null && !(qs instanceof HardSelectQueryStruct)) {
					if ((qs.getSelectors().get(colIndex - 1)
							.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION)) {
						QueryFunctionSelector currentSelect = (QueryFunctionSelector) qs.getSelectors()
								.get(colIndex - 1);
						String aggregate = currentSelect.getFunction();
						rawHeaders[colIndex - 1] = rawHeaders[colIndex - 1].replaceFirst((aggregate.toLowerCase()),
								aggregate);
						headers[colIndex - 1] = headers[colIndex - 1].replaceFirst((aggregate.toLowerCase()),
								aggregate);
					}
				}
				colTypes[colIndex - 1] = rsmd.getColumnType(colIndex);
			}
		} catch (SQLException e) {
			classLogger.error("Error reading column metadata from the Impala result set", e);
		}
	}

}
