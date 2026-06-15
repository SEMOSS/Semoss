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

import java.util.Arrays;
import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.om.HeadersException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UnpivotReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor unpivots columns so that the columns selected will be removed
	 * and combined to generate 2 new columns "variable" and "value" "variable" -
	 * original column headers "value" - value for original column header
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the columns to unpivot</li>
	 * </ul>
	 */

	public UnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String table = frame.getName();

		// get column inputs in an array
		String[] columns = getStringArray();

		// makes the columns and converts them into rows
		// melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		String concatString = "";
		String tempName = Utility.getRandomString(8);
		int numColsToUnPivot = columns.length;
		if (numColsToUnPivot > 0) {
			concatString = ", measure.vars = c(";
			for (int colIndex = 0; colIndex < numColsToUnPivot; colIndex++) {
				concatString = concatString + "\"" + columns[colIndex] + "\"";
				if (colIndex + 1 < numColsToUnPivot) {
					concatString = concatString + ", ";
				}
			}
			concatString = concatString + ")";
		}

		// we want to make sure the new columns that we add
		// are in fact unique
		// so we will loop through and ensure that
		// and also guarantee that they are in sync
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> allColumns = Arrays.asList(frame.getColumnHeaders());
		// we make the assumption that the start headers are already clean
		String[] newColumns = headerChecker.cleanAndMatchColumnNumbers("variable_1", "value_1", allColumns);
		String varName = newColumns[0];
		String valueName = newColumns[1];

		// now that we have unique values
		// we can proceed with the script
		String script = tempName + "<- melt(" + table + ", variable.name = \"" + varName + "\", value.name = \""
				+ valueName + "\"" + concatString + ");";

		// run the first script to unpivot into the temp frame
		frame.executeRScript(script);
		this.addExecutedCode(script);
		// if we are to replace the existing frame
		script = table + " <- " + tempName;
		frame.executeRScript(script);
		this.addExecutedCode(script);

		frame.recreateMeta();
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + tempName + ");");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		this.addExecutedCode(cleanUpScript.toString());

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private String[] getStringArray() {
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
