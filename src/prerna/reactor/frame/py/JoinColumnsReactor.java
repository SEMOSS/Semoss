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

import java.util.Iterator;
import java.util.List;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JoinColumnsReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor joins columns, and puts the joined string into a new column with
	 * values separated by a separator
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the new column name</li>
	 * <li>the delimiter</li>
	 * <li>the columns to join</li>
	 * </ul>
	 */

	public JoinColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String wrapperFrameName = frame.getWrapperName();

		// first input is what we want to name the new column
		String newColName = getStringFromKeyOrCurRow(this.keysToGet[0], 0);
		// check if new colName is valid
		newColName = getCleanNewColName(frame, newColName);

		// second input is the delimeter/separator
		String separator = getStringFromKeyOrCurRow(this.keysToGet[1], 1);

		List<String> columnList = getColumns();
		StringBuilder pyColumnListSB = new StringBuilder();
		pyColumnListSB.append("[");
		for (String column : columnList) {
			// separate the column name from the frame name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}
			pyColumnListSB.append("'" + column + "',");
		}
		pyColumnListSB.append("]");
		String pyColumnList = pyColumnListSB.toString();

		String script = wrapperFrameName + ".join('" + newColName + "', " + pyColumnList + ", '" + separator + "')";
		frame.runScript(script);
		this.addExecutedCode(script);

		recreateMetadata(frame, false);

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private List<String> getColumns() {
		List<String> columns = getListStringFromKeyOrCurRow(this.keysToGet[2], 2);
		Iterator<String> columnIterator = columns.iterator();
		while (columnIterator.hasNext()) {
			String column = columnIterator.next();
			if (column == null || column.isEmpty()) {
				columnIterator.remove();
			}
		}
		return columns;
	}

}
