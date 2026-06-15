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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

public class RunMatrixRegressionReactor extends AbstractRFrameReactor {

	private static Random rand = new Random();

	private static final String CLASS_NAME = RunMatrixRegressionReactor.class.getName();

	private static final String Y_COLUMN = "yColumn";
	private static final String X_COLUMNS = "xColumns";

	public RunMatrixRegressionReactor() {
		this.keysToGet = new String[] { Y_COLUMN, X_COLUMNS, ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);

		// figure out inputs
		List<String> panelIds = getPanelId();
		if (panelIds == null || panelIds.isEmpty()) {
			panelIds = new ArrayList<>();
			String panelId = rand.nextInt(5000) + "";
			panelIds.add(panelId);
			panelId = rand.nextInt(5000) + "";
			panelIds.add(panelId);
		} else if (panelIds.size() < 2) {
			String panelId = rand.nextInt(5000) + "";
			panelIds.add(panelId);
		}
		String predictionCol = getPrediction(logger);
		List<String> numericalCols = getColumns();
		if (numericalCols.contains(predictionCol)) {
			numericalCols.remove(predictionCol);
		}
		int numCols = numericalCols.size();
		if (numCols == 0) {
			String errorString = "Could not find input x variables";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}

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

		// get the correlation data from the run r regression algorithm
		logger.info("Start iterating through data to determine regression");
		String resultsList = runRLinearRegression(frameName, predictionCol, retHeaders, logger);
		logger.info("Done iterating through data to determine regression");

		// Coefficient Table Object

		// the length of the object will be numCols + 1 (because of the intercept)
		// there will always be 2 rows (column header and coefficient)
		Object[][] retCoefficientOutput = new Object[numCols][2];

		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] coefficientTableHeaders = new String[] { "ColumnName", "Coefficient" };

		// query for retrieving the first item of the list - the coefficient table
		String queryCoefficients = resultsList + "[[1]]";
		List<Object[]> bulkRow = this.rJavaTranslator.getBulkDataRow(queryCoefficients, coefficientTableHeaders);
		// each entry into the list is a row - we need to put this in the form of
		// Object[][]
		for (int i = 0; i < bulkRow.size(); i++) {
			retCoefficientOutput[i] = bulkRow.get(i);
		}

		// paint is as grid
		String[] labels = { "ColumnName", "Coefficient" };
		ITask gridTaskData = ConstantTaskCreationHelper.getGridData(panelIds.get(1), labels, retCoefficientOutput);
		NounMetadata noun1 = new NounMetadata(gridTaskData, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);

		// Actuals vs Fitted Object

		// we need to add a unique row id
		String[] dataTableHeaders = new String[] { "ROW_ID", "Actual", "Predicted" };

		// query for retrieving the second item of the list - the Actuals vs Fitted
		String queryDataPoints = resultsList + "[[2]]";
		this.rJavaTranslator.executeEmptyR(queryDataPoints + "$ROW_ID <- seq.int(nrow(" + queryDataPoints + "))");

		// if it has over 10k rows, then sample it
		int rows = this.rJavaTranslator.getInt("nrow(" + queryDataPoints + ")");
		int sampleAmount = 10000;
		if (rows > sampleAmount) {
			String sampleScript = queryDataPoints + " <- as.data.frame(" + queryDataPoints + "[sample(nrow("
					+ queryDataPoints + ")," + sampleAmount + "),])";
			this.rJavaTranslator.executeEmptyR(sampleScript);
		}

		// move to java var
		List<Object[]> bulkRowDataPoints = this.rJavaTranslator.getBulkDataRow(queryDataPoints, dataTableHeaders);

		// create and return a task for the Actuals vs Fitted scatterplot
		ITask scatterTaskData = ConstantTaskCreationHelper.getScatterPlotData(panelIds.get(0), "ROW_ID", "Actual",
				"Fitted", bulkRowDataPoints);
		this.insight.getTaskStore().addTask(scatterTaskData);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + resultsList + "); gc();");

		// now return this object - for the Scatterplot of Actuals vs Fitted
		NounMetadata noun2 = new NounMetadata(scatterTaskData, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		noun2.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Matrix regression ran successfully!"));

		List<NounMetadata> tasks = new ArrayList<NounMetadata>();
		tasks.add(noun1);
		tasks.add(noun2);
		return new NounMetadata(tasks, PixelDataType.VECTOR, PixelOperationType.VECTOR,
				PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
	}

	private String runRLinearRegression(String frameName, String predictionCol, String[] retHeaders, Logger logger) {
		StringBuilder rsb = new StringBuilder();

		// Organize explanatory col headers
		String indColsVector = RSyntaxHelper.createStringRColVec(retHeaders);

		// create a name for the results list; this list will contain two tables:
		// 1) the table of coefficients
		// 2) the table of actuals vs fitted
		String resultsListName = "ResultsList" + Utility.getRandomString(10);

		// source the r script that will run the numerical correlation routine
		String regressionScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\MatrixRegression.R";
		regressionScriptFilePath = regressionScriptFilePath.replace("\\", "/");
		String dataFrameTable = frameName + Utility.getRandomString(6);
		rsb.append("source(\"" + regressionScriptFilePath + "\");");
		rsb.append(RSyntaxHelper.asDataFrame(dataFrameTable + "", frameName));
		// R syntax for the routine: getRegressionCoefficientsFromScript("lm(y~x, data =
		// frameName)", frameName$PredictionCol)
		rsb.append(resultsListName + "<- fit_lm(" + dataFrameTable + ",\"" + predictionCol + "\", " + indColsVector
				+ ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// see how many rows were dropped
		int origRows = this.rJavaTranslator.getInt("nrow(" + dataFrameTable + ")");
		int newRows = this.rJavaTranslator.getInt("nrow(" + resultsListName + "[[2]])");
		int rowsDropped = origRows - newRows;

		// if all rows were dropped, throw error
		if (newRows == 0) {
			String errorString = "Cannot run Matrix Regression on data with 0 non-null rows";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}

		// throw warning to user otherwise
		if (rowsDropped > 0) {
			String errorString = "Dropping " + rowsDropped + " rows due to null values";
			logger.info(errorString);
		}

		// cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + dataFrameTable + "); gc();");
		return resultsListName;
	}

	private List<String> getPanelId() {
		List<String> panelIds = getListString(keysToGet[2]);
		if (panelIds != null && !panelIds.isEmpty()) {
			return panelIds;
		}
		return null;
	}

	private String getPrediction(Logger logger) {
		String prediction = getString(Y_COLUMN);
		if (prediction != null && !prediction.isEmpty()) {
			return prediction;
		}

		// else, throw error
		if (this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find input for variable y";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		return null;
	}

	private List<String> getColumns() {
		List<String> columns = getListString(X_COLUMNS);
		if (columns != null && !columns.isEmpty()) {
			return columns;
		}
		return null;
	}
}
