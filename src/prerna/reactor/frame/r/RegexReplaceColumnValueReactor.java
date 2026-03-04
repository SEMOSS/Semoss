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
package prerna.reactor.frame.r;

import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RegexReplaceColumnValueReactor extends AbstractRFrameReactor {

	/**
	 * This reactor updates row values based on a regex It replaces all portions of
	 * the current cell value that is an exact match to the input value The inputs
	 * to the reactor are: 1) the column to update 2) the regex to look for 3) value
	 * to replace the regex with
	 */

	private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
	private static final String QUOTE = "\"";

	public RegexReplaceColumnValueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.NEW_VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();

		// get inputs
		// first input is the column that we are updating
		List<String> columnNames = getColumns();

		// get regular expression
		String regex = this.keyValue.get(this.keysToGet[1]);
		if (regex == null) {
			regex = getRegex();
		}

		// get new value
		String newValue = this.keyValue.get(this.keysToGet[2]);
		if (newValue == null) {
			newValue = getNewValue();
		}

		// iterate through all passed columns
		StringBuilder script = new StringBuilder();
		for (String column : columnNames) {

			// use method to retrieve a single column type
			String columnSelect = table + "$" + column;
			String colDataType = getColumnType(table, column);
			if (colDataType == null) {
				return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
			}

			SemossDataType sType = SemossDataType.convertStringToDataType(colDataType);

			// script is of the form FRAME$Genre = gsub("-","M", FRAME$Genre)

			if (sType == SemossDataType.INT) {
				// make sure the new value can be properly casted to a number
				if (!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field to non-numeric values");
				}

				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");
				// turn back to int
				script.append(columnSelect + "<- as.integer(" + columnSelect + ");");

			} else if (sType == SemossDataType.DOUBLE) {
				// make sure the new value can be properly casted to a number
				if (!NUMERIC_PATTERN.matcher(newValue).matches()) {
					throw new IllegalArgumentException("Cannot update a numeric field to non-numeric values");
				}

				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");
				// turn back to numeric
				script.append(columnSelect + "<- as.numeric(" + columnSelect + ");");

			} else if (sType == SemossDataType.DATE) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}

				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");
				// turn back to date
				script.append(columnSelect + "<- as.Date(" + columnSelect + ");");

			} else if (sType == SemossDataType.TIMESTAMP) {
				// NOT VALID - WHAT IF I WANT TO UPDATE A MONTH - DAY PORTION ?
//				if(!NUMERIC_PATTERN.matcher(newValue).matches()) {
//					throw new IllegalArgumentException("Cannot update a date field to non-numeric values");
//				}

				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");
				// turn back to timestamp
				script.append(columnSelect + "<- as.POSIXct(" + columnSelect + ");");

			} else if (sType == SemossDataType.STRING) {
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");

			} else if (sType == SemossDataType.FACTOR) {
				script.append(columnSelect + "<- gsub(" + QUOTE + regex + QUOTE + "," + QUOTE + newValue + QUOTE + ", "
						+ columnSelect + ");");
				// turn back to factor
				script.append(columnSelect + "<- as.factor(" + columnSelect + ");");

				// TODO: account for ordered factor ...
				// TODO: account for ordered factor ...
			}
		}
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private List<String> getColumns() {
		List<String> cols = new Vector<String>();

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

	private String getRegex() {
		// second input is the regex
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String regex = input2.getValue() + "";
		return regex;
	}

	private String getNewValue() {
		// third input is the new value
		NounMetadata input3 = this.getCurRow().getNoun(2);
		String newValue = input3.getValue() + "";
		return newValue;
	}

}
