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

import prerna.ds.r.RDataTable;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class DescriptiveStatsReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor gets statistics for a column
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>column to get stats on</li>
	 * <li>panelId (defaults to zero if no panel id is entered)</li>
	 * </ul>
	 */

	public DescriptiveStatsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {

		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String table = frame.getName();

		// get inputs
		String column = getColumn();
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// need panel id to display
		String panelId = getPanelId();

		// create the object to store the output in
		Object[][] retOutput = new Object[8][2]; // name and the number of items
		String frameExpr = table + "$" + column;

		// get minimum
		String script = "min(as.numeric(na.omit(" + frameExpr + ")))";
		double min = this.rJavaTranslator.getDouble(script);
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = min;

		// get quartiles
		script = "quantile(as.numeric(na.omit(" + frameExpr + ")), prob = c(0.25, 0.75))";
		double[] quartiles = this.rJavaTranslator.getDoubleArray(script);
		retOutput[1][0] = "Q1";
		retOutput[1][1] = quartiles[0];
		retOutput[2][0] = "Q3";
		retOutput[2][1] = quartiles[1];

		// get maximum
		script = "max(as.numeric(na.omit(" + frameExpr + ")))";
		double max = this.rJavaTranslator.getDouble(script);
		retOutput[3][0] = "Maximum";
		retOutput[3][1] = max;

		// get mean
		script = "mean(as.numeric(na.omit(" + frameExpr + ")))";
		double mean = this.rJavaTranslator.getDouble(script);
		retOutput[4][0] = "Mean";
		retOutput[4][1] = mean;

		// get median
		script = "median(as.numeric(na.omit(" + frameExpr + ")))";
		double median = this.rJavaTranslator.getDouble(script);
		retOutput[5][0] = "Median";
		retOutput[5][1] = median;

		// get sum
		script = "sum(as.numeric(na.omit(" + frameExpr + ")))";
		double sum = this.rJavaTranslator.getDouble(script);
		retOutput[6][0] = "Sum";
		retOutput[6][1] = sum;

		// get standard deviation
		script = "sd(as.numeric(na.omit(" + frameExpr + ")))";
		double sd = this.rJavaTranslator.getDouble(script);
		retOutput[7][0] = "Standard Deviation";
		retOutput[7][1] = sd;

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
