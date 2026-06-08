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
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RegexReplaceColumnValueReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor updates row values based on a regex It replaces all portions of
	 * the current cell value that is an exact match to the
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the column to update</li>
	 * <li>the regex to look for</li>
	 * <li>value to replace the regex with</li>
	 * </ul>
	 */

	private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

	public RegexReplaceColumnValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.NEW_VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get wrapper name
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		// first input is the column that we are updating
		List<String> columnNames = getColumns();

		// get regular expression
		String regex = this.keyValue.get(this.keysToGet[1]);
		if (regex == null) {
			throw new IllegalArgumentException("Need to define " + this.keysToGet[1]);
		}

		// get new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		if (newValue == null) {
			throw new IllegalArgumentException("Need to define " + this.keysToGet[2]);
		}

		int numColumns = columnNames.size();
		String[] scripts = new String[columnNames.size()];

		// iterate through all passed columns
		for (int i = 0; i < numColumns; i++) {
			String column = columnNames.get(i);
			SemossDataType sType = SemossDataType.convertStringToDataType(getColumnType(frame, column));

			if (sType == SemossDataType.INT || sType == SemossDataType.DOUBLE) {
				// make sure the new value can be properly casted to a number
				if (!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field to non-numeric values");
				}

				// TODO: See why this is not executing properly in python!
				scripts[i] = wrapperFrameName + ".regex_replace_val('" + column + "', " + regex + ", " + newValue + ")";
			} else if (sType == SemossDataType.DATE) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}

				scripts[i] = wrapperFrameName + ".regex_replace_val('" + column + "', '" + regex + "' , '" + newValue
						+ "')";

			} else if (sType == SemossDataType.TIMESTAMP) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}

				scripts[i] = wrapperFrameName + ".regex_replace_val('" + column + "', '" + regex + "' , '" + newValue
						+ "')";

			} else if (sType == SemossDataType.STRING) {
				scripts[i] = wrapperFrameName + ".regex_replace_val('" + column + "', '" + regex + "' , '" + newValue
						+ "')";
			}
		}
		// execute all of the routines after we have done our validation
		insight.getPyTranslator().runEmptyPy(scripts);
		for (String script : scripts) {
			this.addExecutedCode(script);
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				String column = grs.get(i).toString();
				if (column.contains("__")) {
					column = column.split("__")[1];
				}
				cols.add(column);
			}
			return cols;
		}

		return cols;
	}

}
