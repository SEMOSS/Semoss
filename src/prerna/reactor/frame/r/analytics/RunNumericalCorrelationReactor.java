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
package prerna.reactor.frame.r.analytics;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class RunNumericalCorrelationReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RunNumericalCorrelationReactor.class.getName();

	public RunNumericalCorrelationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.DEFAULT_VALUE_KEY.getKey(),
				ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);

		// figure out inputs
		// need the panelId for passing the task to the FE
		String panelId = getString(keysToGet[2]);

		List<String> numericalCols = getListStringFromKeyOrCurRow(keysToGet[0]);
		int numCols = numericalCols.size();
		if (numCols == 0) {
			String errorString = "No columns were passed as attributes for the classification routine.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		// double missingVal = getDefaultValue();

		// need the headers as a list of strings
		String[] retHeaders = new String[numCols];

		for (int i = 0; i < numCols; i++) {
			String header = numericalCols.get(i);
			if (header.contains("__")) {
				String[] split = header.split("__");
				retHeaders[i] = split[1];
			} else {
				retHeaders[i] = header;
			}
		}

		// get the correlation data from the run r correlation algorithm
		logger.info("Start iterating through data to determine correlation");
		String correlationDataTable = runRCorrelation(frameName, retHeaders);
		logger.info("Done iterating through data to determine correlation");

		// create the object to return to the FE
		// the length of the object will be numCols^2
		// there will always be three rows x,y,cor
		int length = numCols * numCols;
		Object[][] retOutput = new Object[length][3];

		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] heatMapHeaders = new String[] { "Column_Header_X", "Column_Header_Y", "Correlation" };
		String query = correlationDataTable + "[" + 1 + ":" + length + "]";
		List<Object[]> bulkRow = this.rJavaTranslator.getBulkDataRow(query, heatMapHeaders);
		// each entry into the list is a row - we need to put this in the form of
		// Object[][]
		for (int i = 0; i < bulkRow.size(); i++) {
			retOutput[i] = bulkRow.get(i);
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getHeatMapData(panelId, "Column_Header_X", "Column_Header_Y",
				"Correlation", retOutput);
		this.insight.getTaskStore().addTask(taskData);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + correlationDataTable + "); gc();");

		// now return this object
		// we are returning the name of our table that sits in R; it is structured as a
		// list of entries: x,y,cor
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Numerical Correlation ran successfully!"));
		return noun;
	}

	private String runRCorrelation(String frameName, String[] retHeaders) {

		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(retHeaders);

		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\NumericalCorrelation.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");
		// R syntax for the routine: ResultsTableName <- getCorrelationTable(frameName,
		// c("col1", "col2"))
		rsb.append(resultsFrameName + "<- getCorrelationTable(" + frameName + ", " + colVector + ")");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

//		// check to verify output
//		String[] colNames = this.rJavaTranslator.getColumns(resultsFrameName);
//		List<Object[]> data = this.rJavaTranslator.getBulkDataRow(resultsFrameName, colNames);
//		
//		for (int i=0; i < data.size(); i++){
//			Object[] val = data.get(i);
//			System.out.println(val[0]);
//			
//		}
		// return the name of the table that sits in r to the FE
		return resultsFrameName;
	}

}
