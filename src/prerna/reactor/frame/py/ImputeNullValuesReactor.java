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
package prerna.reactor.frame.py;

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/*
 * This reactor will impute the null values for the specified columns.
 * For a numeric valued column, nulls will be replaced by the column mean.
 * For date-times, the value will be forward filled, or replaced by the last valid observation
 * For Strings, null values will be replaced by the mode of the column or the string "Unknown" if no mode exists
 */
public class ImputeNullValuesReactor extends AbstractPyFrameReactor {

	protected static final String CLASS_NAME = ImputeNullValuesReactor.class.getName();

	public ImputeNullValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperFrameName = frame.getWrapperName();
		String wrapperDataIndexed = wrapperFrameName + ".cache['data']";

		// get inputs
		// first input is the column that we are updating
		List<String> columnNames = getColumns();

		List<String> scripts = new ArrayList<>();

		// iterate through all passed columns
		for (String column : columnNames) {
			SemossDataType columnDataType = SemossDataType.convertStringToDataType(getColumnType(frame, column));
			if (columnDataType == SemossDataType.INT || columnDataType == SemossDataType.DOUBLE) {
				scripts.add(wrapperDataIndexed + "['" + column + "'] = " + wrapperDataIndexed + "['" + column
						+ "'].fillna(" + wrapperDataIndexed + "['" + column + "'].mean(skipna=True))");
			} else if (columnDataType == SemossDataType.DATE || columnDataType == SemossDataType.TIMESTAMP) {
				scripts.add(wrapperDataIndexed + "['" + column + "'].fillna(value=pd.to_datetime(" + wrapperDataIndexed
						+ "['" + column + "']).ffill(),inplace=True)");
			} else if (columnDataType == SemossDataType.STRING) {
				String modeconditional = "'Unknown' if " + wrapperDataIndexed + "['" + column
						+ "'].mode(dropna=True).empty else " + wrapperDataIndexed + "['" + column
						+ "'].mode(dropna=True)";
				scripts.add(wrapperDataIndexed + "['" + column + "'].fillna(" + modeconditional + ", inplace=True)");
			}
		}

		// execute all of the routines after we have done our validation
		insight.getPyTranslator().runEmptyPy(scripts.toArray(new String[0]));
		for (String script : scripts) {
			this.addExecutedCode(script);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<String> getColumns() {
		List<String> cols = getListStringFromKeyOrCurRow(ReactorKeysEnum.COLUMNS.getKey());
		if (!cols.isEmpty()) {
			return cols;
		}
		throw new IllegalArgumentException("Need to define the columns to impute");
	}
}
