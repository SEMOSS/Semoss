package prerna.reactor.frame.r.analytics;

import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunMatrixRegressionReactor extends AbstractRFrameReactor {

	private static Random rand = new Random();

	private static final String CLASS_NAME = RunMatrixRegressionReactor.class.getName();

	private static final String Y_COLUMN = "yColumn";
	private static final String X_COLUMNS = "xColumns";

	public RunMatrixRegressionReactor() {
		this.keysToGet = new String[]{Y_COLUMN, X_COLUMNS, ReactorKeysEnum.PANEL.getKey()};
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
		if(panelIds == null || panelIds.isEmpty()) {
			panelIds = new Vector<String>();
			String panelId = rand.nextInt(5000) + "";
			panelIds.add(panelId);
			panelId = rand.nextInt(5000) + "";
			panelIds.add(panelId);
		} else if(panelIds.size() < 2) {
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
		String resultsList = runRLinearRegression(frameName, predictionCol, retHeaders, logger);
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

		// THIS DOES NOT GET USED:

		// the length of the object will be numCols + 1 (because of the intercept)
		// there will always be 2 rows (column header and coefficient)
		Object[][] retCoefficientOutput = new Object[numCols][2];

		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] coefficientTableHeaders = new String[]{"ColumnName", "Coefficient"};

		// query for retrieving the first item of the list - the coefficient table
		String queryCoefficients = resultsList + "[[1]]";
		List<Object[]> bulkRow = this.rJavaTranslator.getBulkDataRow(queryCoefficients, coefficientTableHeaders);
		// each entry into the list is a row - we need to put this in the form of Object[][]
		for (int i = 0; i < bulkRow.size(); i++) {
			retCoefficientOutput[i] = bulkRow.get(i);
		}

		// paint is as grid
		String[] labels = {"ColumnName", "Coefficient"};
		ITask gridTaskData = ConstantTaskCreationHelper.getGridData(panelIds.get(1), labels, retCoefficientOutput);		
		NounMetadata noun1 = new NounMetadata(gridTaskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);

		/////////////////////////////////////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////////////
		////////////////////////Actuals vs Fitted Object/////////////////////////////
		////////////////////////////////////////////////////////////////////////////

		// we need to add a unique row id
		String[] dataTableHeaders = new String[]{"ROW_ID", "Actual", "Predicted"};

		// query for retrieving the second item of the list - the Actuals vs Fitted
		String queryDataPoints = resultsList + "[[2]]";
		this.rJavaTranslator.executeEmptyR(queryDataPoints + "$ROW_ID <- seq.int(nrow(" + queryDataPoints + "))");

		// if it has over 10k rows, then sample it
		int rows = this.rJavaTranslator.getInt("nrow(" + queryDataPoints + ")");
		int sampleAmount = 10000;
		if(rows>sampleAmount) {
			String sampleScript = queryDataPoints + " <- as.data.frame(" + queryDataPoints + "[sample(nrow("
					+ queryDataPoints + ")," + sampleAmount + "),])";
			this.rJavaTranslator.executeEmptyR(sampleScript);
		}

		// move to java var
		List<Object[]> bulkRowDataPoints = this.rJavaTranslator.getBulkDataRow(queryDataPoints, dataTableHeaders);

		// create and return a task for the Actuals vs Fitted scatterplot
		ITask scatterTaskData = ConstantTaskCreationHelper.getScatterPlotData(panelIds.get(0), "ROW_ID", "Actual", "Fitted", bulkRowDataPoints);
		this.insight.getTaskStore().addTask(scatterTaskData);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + resultsList + "); gc();");

		// now return this object - for the Scatterplot of Actuals vs Fitted
		NounMetadata noun2 = new NounMetadata(scatterTaskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun2.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Matrix regression ran successfully!"));

		List<NounMetadata> tasks = new Vector<NounMetadata>();
		tasks.add(noun1);
		tasks.add(noun2);
		return new NounMetadata(tasks, PixelDataType.VECTOR, PixelOperationType.VECTOR, 
				PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Running regression R script
	 */

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
		// R syntax for the routine: getRegressionCoefficientsFromScript("lm(y~x, data = frameName)", frameName$PredictionCol)
		rsb.append(resultsListName + "<- fit_lm(" + dataFrameTable + ",\"" + predictionCol + "\", " + indColsVector +  ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// see how many rows were dropped
		int origRows = this.rJavaTranslator.getInt("nrow(" + dataFrameTable + ")");
		int newRows = this.rJavaTranslator.getInt("nrow(" + resultsListName + "[[2]])");
		int rowsDropped = origRows - newRows;

		// if all rows were dropped, throw error
		if(newRows==0) {
			String errorString = "Cannot run Matrix Regression on data with 0 non-null rows";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}

		// throw warning to user otherwise
		if(rowsDropped > 0) {
			String errorString = "Dropping " + rowsDropped + " rows due to null values";
			logger.info(errorString);
		}
		
		// cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + dataFrameTable + "); gc();");
		return resultsListName;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Retrieving inputs
	 */

	private List<String> getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if(columnGrs != null && !columnGrs.isEmpty()) {
			return columnGrs.getAllStrValues();
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

		// else, throw error
		if(this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find input for variable y";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		return null;
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
