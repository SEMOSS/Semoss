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

import java.util.Arrays;
import java.util.List;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.om.HeadersException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UnpivotReactor extends AbstractPyFrameReactor {

	public UnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperName = frame.getWrapperName();
		// get column inputs in an array
		String[] columns = getColumns();
		String frameName = frame.getName();

		// makes the columns and converts them into
		// list ['col1', 'col2']
		StringBuilder valueColumns = new StringBuilder();
		valueColumns.append("[");
		for (int i = 0; i < columns.length; i++) {
			valueColumns.append("'" + columns[i] + "'");
			if (i + 1 < columns.length) {
				valueColumns.append(", ");
			}
		}
		valueColumns.append("]");

		String script = frameName + " = " + wrapperName + ".unpivot(" + valueColumns + ")";
		frame.runScript(script);
		this.addExecutedCode(script);

		HeadersException headerChecker = HeadersException.getInstance();
		List<String> allColumns = Arrays.asList(getColumns(frame));
		// python unpivot creates two columns variable and value
		// we make the assumption that the start headers
		// are already clean and not duplicating
		String variableName = "variable";
		String valueName = "value";
		String[] newColumns = headerChecker.cleanAndMatchColumnNumbers(variableName, valueName, allColumns);
		String newVarName = newColumns[0];
		String newValueName = newColumns[1];

		// rename variable name
		String rename = PandasSyntaxHelper.alterColumnName(frameName, variableName, newVarName);
		frame.runScript(rename);
		this.addExecutedCode(rename);
		// rename value name
		rename = PandasSyntaxHelper.alterColumnName(frameName, valueName, newValueName);
		frame.runScript(rename);
		this.addExecutedCode(rename);

		frame = (PandasFrame) recreateMetadata(frame);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String[] getColumns() {
		List<String> keyColumns = getListString(this.keysToGet[0]);
		if (keyColumns != null && !keyColumns.isEmpty()) {
			return keyColumns.toArray(new String[0]);
		}

		List<String> columns = getCurRowValuesAsString();
		if (!columns.isEmpty()) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				if (column != null && column.contains("__")) {
					column = column.split("__")[1];
				}
				columns.set(i, column);
			}
			return columns.toArray(new String[0]);
		}

		throw new IllegalArgumentException("Need to define columns to unpivot");
	}
}
