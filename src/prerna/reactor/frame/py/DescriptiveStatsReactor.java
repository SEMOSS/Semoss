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

import java.util.ArrayList;
import java.util.HashMap;

import prerna.ds.py.PandasFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class DescriptiveStatsReactor extends AbstractFrameReactor {

	/**
	 * Retrieves statistics for a column.
	 *
	 * <p>
	 * Expected inputs:
	 * <ul>
	 * <li><b>column</b> - The column for which statistics should be
	 * calculated.</li>
	 * <li><b>panelId</b> - The panel identifier. Defaults to {@code 0} if not
	 * provided.</li>
	 * </ul>
	 */

	public DescriptiveStatsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {

		// initialize the rJavaTranslator
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get frame name
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		String column = getColumn();
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// need panel id to display
		String panelId = getPanelId();

		HashMap output = (HashMap) frame.runScript(wrapperFrameName + ".stat('" + column + "')");
		ArrayList sum_median = (ArrayList) frame.runScript(wrapperFrameName + ".sum_median('" + column + "')");

		// if(output.size() == 8) // this was numeric and seems like we can only handle
		// that ?
		// {

		// create the object to store the output in
		Object[][] retOutput = new Object[8][2]; // name and the number of items

		// get minimum
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = ((HashMap) output.get("min")).get(column);

		// get quartiles
		retOutput[1][0] = "Q1";
		retOutput[1][1] = ((HashMap) output.get("25%")).get(column);
		retOutput[2][0] = "Q3";
		retOutput[2][1] = ((HashMap) output.get("75%")).get(column);

		// get maximum
		retOutput[3][0] = "Maximum";
		retOutput[3][1] = ((HashMap) output.get("max")).get(column);

		// get mean
		retOutput[4][0] = "Mean";
		retOutput[4][1] = ((HashMap) output.get("mean")).get(column);

		// get median
		retOutput[5][0] = "Median";
		retOutput[5][1] = sum_median.get(1);

		// get sum
		retOutput[6][0] = "Sum";
		retOutput[6][1] = sum_median.get(0);

		// get standard deviation
		retOutput[7][0] = "Standard Deviation";
		retOutput[7][1] = ((HashMap) output.get("std")).get(column);

		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "StatOutput", retOutput);
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

	private String getColumn() {
		String column = getString(keysToGet[0]);
		if (column != null && !column.isEmpty()) {
			return column;
		}
		throw new IllegalArgumentException("Need to define column for descriptive statistics");
	}

	// get panel id using key "PANEL"
	private String getPanelId() {
		return getString(keysToGet[1], "0");
	}
}
