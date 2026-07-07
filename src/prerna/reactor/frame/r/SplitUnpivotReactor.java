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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SplitUnpivotReactor extends AbstractRFrameReactor {

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
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMNS.getKey(),
				ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
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
		// script to change the name of the table back to the original name -
		// will be used later
		String frameReplaceScript = table + " <- " + tempName + ";";
		// false columnReplaceScript indicates that we will not drop the
		// original column of data
		String columnReplaceScript = "FALSE";
		String direction = "long";

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

			String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
			if (dataType == null) {
				return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
			}

			// build the script to execute
			String script = tempName + " <- cSplit(" + table + ", " + "\"" + column + "\", \"" + delimiter
					+ "\", direction = \"" + direction + "\", drop = " + columnReplaceScript + ");";

			// evaluate the r script
			frame.executeRScript(script);
			this.addExecutedCode(script);

			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			// keep track of which columns are factors
			int[] factors = this.rJavaTranslator.getIntArray(script);
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

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<String> getDelimiters() {
		List<String> delimiters = getListString(keysToGet[2]);
		if (delimiters != null && !delimiters.isEmpty()) {
			return delimiters;
		}
		throw new IllegalArgumentException("Need to define delimiters");
	}

	private List<String> getColumns() {
		List<String> columns = getListString(keysToGet[1]);
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
