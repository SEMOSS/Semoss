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

import java.util.List;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SplitColumnsReactor extends AbstractPyFrameReactor {

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
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DELIMITER.getKey(),
				SEARCH_TYPE };
	}

	@Override
	public NounMetadata execute() {
		List<String> cols = getColumns();
		String separator = getSeparator();
		boolean isRegdex = isRegex();

		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String wrapperFrameName = frame.getWrapperName();

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

			// eval py script
			String script = wrapperFrameName + ".split('" + column + "', '" + separator + "')";
			frame.runScript(script);
			this.addExecutedCode(script);
		}

		// column header data is changing so we must recreate metadata
		recreateMetadata(frame, false);

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String getSeparator() {
		String separator = getString(keysToGet[1]);
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
		List<String> cols = getListStringFromKeyOrCurRow(keysToGet[0]);
		if (!cols.isEmpty()) {
			return cols;
		}
		throw new IllegalArgumentException("Need to define the columns to split");
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SEARCH_TYPE)) {
			return "The type of search: Regex or an Exact Match";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
