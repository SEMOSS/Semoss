package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.algorithms.MatrixRegressionReactor;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RMatrixRegressionReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = MatrixRegressionReactor.class.getName();

	private static final String Y_COLUMN = "yColumn";
	private static final String X_COLUMNS = "xColumns";
	
	public RMatrixRegressionReactor() {
		this.keysToGet = new String[]{Y_COLUMN, X_COLUMNS, ReactorKeysEnum.PANEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		
		// figure out inputs
		String panelId = getPanelId();
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
		for(int i = 0; i < numCols; i++) {
			String header = numericalCols.get(i);
			if(header.contains("__")) {
				String[] split = header.split("__");
				retHeaders[i] = split[1];
			} else {
				retHeaders[i] = header;
			}
		}
		
		// get the correlation data from the run r regression algorithm
		logger.info("Start iterating through data to determine regression");
		String resultsList = runRMatrixRegression(frameName, predictionCol, retHeaders);
		logger.info("Done iterating through data to determine regression");
		
		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "MatrixRegression");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"MatrixRegression", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		/////////////////////////////////////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////////////
		////////////////////////Coefficient Table Object/////////////////////////////
		////////////////////////////////////////////////////////////////////////////

		// the length of the object will be numCols + 1 (because of the intercept)
		// there will always be 2 rows (column header and coefficient)
		int length = numCols + 1;
		Object[][] retCoefficientOutput = new Object[length][2];
		
		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] coefficientTableHeaders = new String[]{"Column Header", "Coefficient"};
		
		// query for retrieving the first item of the list - the coefficient table
		String queryCoefficients = resultsList + "[[1]]" + "[" + 1 + ":" + length + "]";
		List<Object[]> bulkRow = this.rJavaTranslator.getBulkDataRow(queryCoefficients, coefficientTableHeaders);
		// each entry into the list is a row - we need to put this in the form of Object[][]
		for (int i = 0; i < bulkRow.size(); i++) {
			retCoefficientOutput[i] = bulkRow.get(i);
		}
		
		/////////////////////////////////////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////////////
		////////////////////////Actuals vs Fitted Object/////////////////////////////
		////////////////////////////////////////////////////////////////////////////
		
		// we need to add a unique row id
		String[] dataTableHeaders = new String[]{"ROW_ID", "Actual", "Fitted"};
		
		// query for retrieving the second item of the list - the Actuals vs Fitted
		String queryDataPoints = resultsList + "[[2]]";
		this.rJavaTranslator.executeEmptyR(queryDataPoints + "$ROW_ID <- seq.int(nrow(" + queryDataPoints + "))");
		List<Object[]> bulkRowDataPoints = this.rJavaTranslator.getBulkDataRow(queryDataPoints, dataTableHeaders);
		
		// create and return a task for the Actuals vs Fitted scatterplot
		ITask taskData = ConstantTaskCreationHelper.getScatterPlotData(panelId, "ROW_ID", "Actual", "Fitted", bulkRowDataPoints);
		this.insight.getTaskStore().addTask(taskData);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + resultsList + "); gc();");

		// now return this object - for the Scatterplot of Actuals vs Fitted
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(
				new NounMetadata("Matrix regression ran successfully!", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Running regression R script
	 */
	
	private String runRMatrixRegression(String frameName, String predictionCol, String[] retHeaders) {
		
		// stringbuilder for the lm function string
		// we will pass this into the r method
		StringBuilder functionInput = new StringBuilder();
		// the format of the lm function is: lm(y ~ x1 + x2 + x3, data = frameName)
		functionInput.append("lm(").append(frameName).append("$").append(predictionCol).append(" ~ ");
		
		// iteratively add each column name based on the retHeaders
		for (int i = 0 ; i < retHeaders.length; i++) {
			functionInput.append(frameName).append("$").append(retHeaders[i]);
			if (i < retHeaders.length - 1) {
				functionInput.append(" + ");
			}
		}
		functionInput.append(", data = ").append(frameName).append(")");

		// create a name for the results list; this list will contain two tables: 
		// 1) the table of coefficients
		// 2) the table of actuals vs fitted
		String resultsListName = "ResultsList" + Utility.getRandomString(10);

		// stringbuilder for the code we will pass to r 
		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String regressionScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\MatrixRegression.R";
		regressionScriptFilePath = regressionScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + regressionScriptFilePath + "\");");
		// R syntax for the routine: getRegressionCoefficientsFromScript("lm(y~x, data = frameName)", frameName$PredictionCol)
		rsb.append(resultsListName + "<- getRegressionCoefficientsFromScript(" + "\"" + functionInput.toString() + "\", " + frameName + "$" +  predictionCol +  ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());
		return resultsListName;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Retrieving inputs
	 */
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}
	
	private String getPrediction(Logger logger) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(Y_COLUMN);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		
		// else, we assume it is the first column
		if(this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find input for variable y";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}

	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(X_COLUMNS);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
	   return null;
	}
}
