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
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class ColumnCountReactor extends AbstractPyFrameReactor {

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
		PandasFrame frame = (PandasFrame) getFrame();
		// get wrapper name
		String wrapperName = frame.getWrapperName();

		List output = (List) frame.runScript(wrapperName + ".get_hist('" + column + "')");
		// create the object with the right size
		// the length will be the same as the number of unique values in the column
		List keys = (List) output.get(0);
		List vals = (List) output.get(1);
		Object[][] retOutput = new Object[keys.size()][2];
		for (int outputIndex = 0; outputIndex < keys.size(); outputIndex++) {
			// we are storing each uniqe col val and its frequency
			retOutput[outputIndex][0] = keys.get(outputIndex);
			retOutput[outputIndex][1] = vals.get(outputIndex);
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "Frequency", retOutput);
		// variable cleanup
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

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

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TOP)) {
			return "Indicates if a column should be sorted by descending frequency";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
