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

import org.rosuda.JRI.RFactor;

import prerna.ds.r.RDataTable;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class ColumnCountReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor counts the number of columns and unique columns it stores these
	 * values in a matrix
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>column to use</li>
	 * <li>boolean indicator (optional) if true (default), sort by descending
	 * frequency of items in a column if false, sort ascending</li>
	 * <li>panelId (defaults to zero if nothing is entered)</li>
	 * </ul>
	 */

	private static final String TOP = "top";

	public ColumnCountReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), TOP, ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		// get inputs
		String column = getColumn();
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		// get boolean top variable
		boolean top = getTop();
		// get panel id in order to display
		String panelId = getPanelId();

		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get frame name
		String table = frame.getName();

		String colType = this.rJavaTranslator.getColumnType(table, column);
		if (colType == null) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}

		if (colType.equals("int") || colType.equals("num") || colType.equals("numeric")) {
			// this accounts for the case that the values are numeric - just make a
			// histogram using the histogram reactor
			// without this section we get errors because the column values are not string
			// or factors
			HistogramReactor histogram = new HistogramReactor();
			// define the number of breaks to build the histogram
			int numBreaks = 0;
			// the get histogram method return nounmetadata
			return histogram.getHistogram(this.rJavaTranslator, table, column, panelId, numBreaks);
		}

		// create temporary table
		String tempName = Utility.getRandomString(6);
		// define r script to be executed
		// this script will create a table with one column of col vals and one column of
		// the corresponding frequency
		String script = null;

		// sort based on boolean top variable; if true, sort descending
		// more frequent items in the column will appear first
		if (top) {
			script = tempName + " <-  head(" + table + "[, .N, by=\"" + column + "\"][order(-rank(N)),] , 25);";
		} else {
			script = tempName + " <-  head(" + table + "[, .N, by=\"" + column + "\"][order(rank(N)),] , 25);";
		}
		this.rJavaTranslator.executeEmptyR(script);

		// store the values of the column in a string array
		// get the column names
		if (colType.equals("ordered") || colType.equals("factor")) {
			script = "as.character(" + tempName + "$" + column + ")";
		} else {
			script = tempName + "$" + column;
		}
		String[] uniqueColumns = null;
		if (colType.equalsIgnoreCase("date")) {
			String dateFormat = "%Y-%m-%d";
			uniqueColumns = this.rJavaTranslator.getStringArray("format(" + script + ", format='" + dateFormat + "')");
		} else {
			uniqueColumns = this.rJavaTranslator.getStringArray(script);
		}
		// if its still null
		// we have a factor
		if (uniqueColumns == null) {
			RFactor factors = (RFactor) this.rJavaTranslator.getFactor(script);
			int numFactors = factors.size();
			uniqueColumns = new String[numFactors];
			for (int i = 0; i < numFactors; i++) {
				uniqueColumns[i] = factors.at(i);
			}
		}

		// this will store a count of each values occurrence in the column
		script = tempName + "$N";
		int[] colCount = this.rJavaTranslator.getIntArray(script);

		// create the object with the right size
		// the length will be the same as the number of unique values in the column
		Object[][] retOutput = new Object[uniqueColumns.length][2];

		for (int outputIndex = 0; outputIndex < uniqueColumns.length; outputIndex++) {
			// we are storing each uniqe col val and its frequency
			retOutput[outputIndex][0] = uniqueColumns[outputIndex];
			retOutput[outputIndex][1] = colCount[outputIndex];
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "Frequency", retOutput);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + tempName + "); gc();");
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getColumn() {
		String column = getString(keysToGet[0]);
		if (column != null && !column.isEmpty()) {
			return column;
		}
		throw new IllegalArgumentException("Need to define column for column count");
	}

	private boolean getTop() {
		String topString = getString(TOP);
		if (topString != null) {
			return !topString.equalsIgnoreCase("false");
		}
		// default to true
		return true;
	}

	// get panel id using key "PANEL"
	private String getPanelId() {
		return getString(keysToGet[2], "0");
	}

	////////////////////////////////// KEYS////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TOP)) {
			return "Indicates if a column should be sorted by descending frequency";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
