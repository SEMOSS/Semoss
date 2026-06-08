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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SplitUnpivotReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor splits columns based on a separator The split values will be
	 * combined into a single column
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the columns to split "columns"</li>
	 * <li>the delimiters "delimiters"</li>
	 * </ul>
	 */

	public SplitUnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String wrapperFrameName = frame.getWrapperName();

		// get the columns
		// already cleaned to exclude the frame name
		List<String> columns = getColumns();

		// get the delimiters
		List<String> delimiters = getDelimiters();

		// throw an error if the number of delimiters doesn't make sense
		// delimiters must match the number of columns, or just use a single
		// delimiter
		if ((columns.size() != delimiters.size()) && delimiters.size() != 1) {
			throw new IllegalArgumentException(
					"Need to enter a single delimiter for all columns or one for each column");
		}

		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			String delimiter = "";
			if (delimiters.size() == 1) {
				delimiter = delimiters.get(0);
			} else {
				delimiter = delimiters.get(i);
			}

			// split_unpivot(column, delimiter)
			// build the script to execute
			String script = wrapperFrameName + ".split_unpivot('" + column + "', '" + delimiter + "')";
			frame.runScript(script);
			this.addExecutedCode(script);
		}
		// update the frame reference as well since these changes modify the object
		String script = frame.getName() + " = " + wrapperFrameName + ".cache['data']";
		frame.runScript(script);
		this.addExecutedCode(script);

		// column header data is changing so we must recreate metadata
		recreateMetadata(frame, false);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<String> getDelimiters() {
		List<String> delimiters = getListString(keysToGet[1]);
		if (delimiters != null && !delimiters.isEmpty()) {
			return delimiters;
		}
		throw new IllegalArgumentException("Need to define delimiters");
	}

	private List<String> getColumns() {
		List<String> columns = getListString(keysToGet[0]);
		if (columns != null && !columns.isEmpty()) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				if (column.contains("__")) {
					column = column.split("__")[1];
				}
				columns.set(i, column);
			}
			return columns;
		}
		throw new IllegalArgumentException("Need to define columns");
	}

}
