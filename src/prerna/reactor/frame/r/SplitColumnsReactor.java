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

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SplitColumnsReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor splits columns based on a separator It replaces all portions of
	 * the current cell value that is an exact match to the
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the separator</li>
	 * <li>the columns to split</li>
	 * </ul>
	 */

	private static final String SEARCH_TYPE = "search";
	private static final String REGEX = "Regex";

	public SplitColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMNS.getKey(),
				ReactorKeysEnum.DELIMITER.getKey(), SEARCH_TYPE };
	}

	@Override
	public NounMetadata execute() {
		List<String> cols = getColumns();
		String separator = getSeparator();
		boolean isRegex = isRegex();

		// use init to initialize rJavaTranslator object that will be used later
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// get table name
		String table = frame.getName();

		// make a temporary table name
		// we will reassign the table to this variable
		// then assign back to the original table name
		String tempName = Utility.getRandomString(8);
		// script to change the name of the table back to the original name - will be
		// used later
		String frameReplaceScript = table + " <- " + tempName + ";";
		// false columnReplaceScript indicates that we will not drop the
		// original column of data
		String columnReplaceScript = "FALSE";
		String direction = "wide";

		// get length of input to use when iterating through
		int inputSize = cols.size();

		for (int i = 0; i < inputSize; i++) {
			// next input will be the column that we are splitting
			// we can specify to split more than one column, so there could be
			// multiple column inputs
			String column = cols.get(i);
			// clean column name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}

			String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
			if (dataType == null) {
				return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
			}

			// build the script to execute
			String script = tempName + " <- cSplit(" + table + ", " + "\"" + column + "\", \"" + separator
					+ "\", direction = \"" + direction + "\", drop = " + columnReplaceScript;
			if (isRegex) {
				script += ", fixed = FALSE";
			} else {
				script += ", fixed = TRUE";
			}
			script += ");";

			// evaluate the r script
			frame.executeRScript(script);
			this.addExecutedCode(script);

			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			// keep track of which columns are factors
			int[] factors = this.rJavaTranslator.getIntArray(script);
			this.addExecutedCode(script);
			String[] colNames = getColumns(tempName);

			// now I need to compose a string based on it
			// we will convert the columns that are factors into strings using
			// as.character
			String conversionString = "";
			for (int factorIndex = 0; factorIndex < factors.length; factorIndex++) {
				if (factors[factorIndex] == 1) // this is a factor
				{
					conversionString = conversionString + tempName + "$" + colNames[factorIndex] + " <- "
							+ "as.character(" + tempName + "$" + colNames[factorIndex] + ");";
				}
			}

			// convert factors
			frame.executeRScript(conversionString);
			this.addExecutedCode(conversionString);
			// change table back to original name
			frame.executeRScript(frameReplaceScript);
			this.addExecutedCode(frameReplaceScript);
			// perform variable cleanup
			String cleanup = "rm(" + tempName + "); gc();";
			frame.executeRScript(cleanup);
			this.addExecutedCode(cleanup);
		}

		// column header data is changing so we must recreate metadata
		frame.recreateMeta();
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String getSeparator() {
		String separator = getString(keysToGet[2]);
		if (separator == null || separator.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		return separator;
	}

	private boolean isRegex() {
		GenRowStruct regexGrs = this.store.getGenRowStruct(SEARCH_TYPE);
		if (regexGrs == null || regexGrs.isEmpty()) {
			return true;
		}
		String val = regexGrs.get(0).toString();
		if (val.equalsIgnoreCase(REGEX)) {
			return true;
		}
		return false;
	}

	private List<String> getColumns() {
		List<String> cols = getListStringFromKeyOrCurRow(keysToGet[1]);
		if (!cols.isEmpty()) {
			return cols;
		}
		throw new IllegalArgumentException("Need to define the columns to split");
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SEARCH_TYPE)) {
			return "The type of search: Regex or an Exact Match";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
