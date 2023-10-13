package prerna.reactor.algorithms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunMatrixRegressionReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(RunMatrixRegressionReactor.class);

	private static final String CLASS_NAME = RunMatrixRegressionReactor.class.getName();

	private static final String Y_COLUMN = "yColumn";
	private static final String X_COLUMNS = "xColumns";
	
	public RunMatrixRegressionReactor() {
		this.keysToGet = new String[]{Y_COLUMN, X_COLUMNS, ReactorKeysEnum.DEFAULT_VALUE_KEY.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		// figure out inputs
		String predictionCol = getPrediction(logger);
		List<String> numericalCols = getColumns();
		if(numericalCols.contains(predictionCol)) {
			numericalCols.remove(predictionCol);
		}
		int numCols = numericalCols.size();
		if(numCols == 0) {
			String errorString = "Could not find input x variables";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		double missingVal = getDefaultValue();

		// I need to return back headers
		// and a dataTableAlign object
		// in addition to the specific correlation data
		String[] retHeaders = new String[numCols+1];
		Map<String, String> dataTableAlign = new HashMap<>();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		// add the predictor column
		QueryColumnSelector predictorHead = new QueryColumnSelector();
		if(predictionCol.contains("__")) {
			String[] split = predictionCol.split("__");
			predictorHead.setTable(split[0]);
			predictorHead.setColumn(split[1]);
			retHeaders[0] = split[1];
		} else {
			predictorHead.setTable(predictionCol);
			retHeaders[0] = predictionCol;
		}
		dataTableAlign.put("dim 0", retHeaders[0]);
		qs.addSelector(predictorHead);
		// add the feature columns
		for(int i = 0; i < numCols; i++) {
			String header = numericalCols.get(i);
			QueryColumnSelector qsHead = new QueryColumnSelector();
			if(header.contains("__")) {
				String[] split = header.split("__");
				qsHead.setTable(split[0]);
				qsHead.setColumn(split[1]);
				retHeaders[i+1] = split[1];
			} else {
				qsHead.setTable(header);
				retHeaders[i+1] = header;
			}
			dataTableAlign.put("dim " + (i+1), retHeaders[i+1]);
			qs.addSelector(qsHead);
		}
		qs.mergeImplicitFilters(dataFrame.getFrameFilters());

		int numRows = getNumRows(dataFrame, predictorHead);
		
		// use apache commons to do this
		// while we need to iterate through to create the double[][]
		// it is still better than implementing matrix math...
		OLSCalculator ols = new OLSCalculator();
		// execute the ols
		ols.setNoIntercept(false);
		double[][] rowData = null;
		IRawSelectWrapper it;
		try {
			it = dataFrame.query(qs);
			logger.info("Start iterating through data to determine regression");
			rowData = setValuesInOlsAndCorr(ols, it, numCols, numRows, missingVal, logger);
			logger.info("Done iterating through data to determine regression");
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}

		if (rowData == null) {
			throw new NullPointerException("row data cannot be null here.");
		}

		// mock the data output
		Map<String, Object> vizData = new HashMap<>();
		vizData.put("data", rowData);
		vizData.put("headers", retHeaders);
		vizData.put("layout", "ScatterplotMatrix");
		vizData.put("panelId", getPanelId());
		vizData.put("dataTableAlign", dataTableAlign);
		// finally, i send the matrix data
		Map<String, Object> specificData = new HashMap<>();
		specificData.put("one-row", true);
		specificData.put("coefficients", ols.getCoefArray());
		specificData.put("r2", ols.calculateRSquared());
		double sumOfResiduals = ols.calculateResidualSumOfSquares();
		specificData.put("sumOfResidual", sumOfResiduals);
		specificData.put("shift", sumOfResiduals / rowData.length);
		// also calculate the correlation of the predictor to all other columns
		PearsonsCorrelation corr = new PearsonsCorrelation(rowData);
		double[][] corrMatrix = corr.getCorrelationMatrix().getData();
		specificData.put("correlations", corrMatrix[0]);
		vizData.put("specificData", specificData);
		// now return this object

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "MatrixRegression");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"MatrixRegression", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
	}
	
	private double[][] setValuesInOlsAndCorr(OLSMultipleLinearRegression ols, 
			Iterator<IHeadersDataRow> it, 
			int numCols, 
			int numRows, 
			double defaultVal,
			Logger logger) 
	{
		double[][] rowData = new double[numRows][numCols]; 
		double[][] x = new double[numRows][numCols];
		double[] y = new double[numRows];

		int counter = 0;
		while(it.hasNext()) {
			Object[] row = it.next().getValues();
			double[] cleanRow = new double[numCols+1];

			// index 0 is the value for y
			y[counter] = getDouble(row[0], defaultVal);
			cleanRow[0] = y[counter];

			// get a clean version of the data while
			// fixing missing values
			double[] doubleRow = new double[numCols];
			for(int i = 0; i < numCols; i++) {
				doubleRow[i] = getDouble(row[i+1], defaultVal);
				// remember, first index is the y in the cleanRow
				cleanRow[i+1] = doubleRow[i];
			}
			// add to A
			x[counter] = doubleRow;
			
			// and add it to rowData so we can send a single matrix to the FE
			rowData[counter] = cleanRow;
			
			if(counter % 100 == 0) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Finished row number = " + counter);
				Configurator.setLevel(logger.getName(), Level.OFF);
			}
			
			// increase the counter for the next row
			counter++;
		}
		
		// set the data within the OLS
		Configurator.setLevel(logger.getName(), Level.INFO);
		ols.newSampleData(y, x);
		return rowData;
	}
	
	private int getNumRows(ITableDataFrame frame, QueryColumnSelector predictorCol) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector math = new QueryFunctionSelector();
		math.addInnerSelector(predictorCol);
		math.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(math);
		
		IRawSelectWrapper countIt = null;
		try {
			countIt = frame.query(qs);
			while (countIt.hasNext()) {
				return ((Number) countIt.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(countIt != null) {
				try {
					countIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return 0;
	}
	
	////////////////////////////////////////////////////////////////
	
	/*
	 * Get input values for algorithm
	 */

	private double getDouble(Object obj, double defaultVal) {
		if(obj instanceof Number) {
			return ((Number) obj).doubleValue();
		}
		return defaultVal;
	}
	
	private String getPrediction(Logger logger) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(Y_COLUMN);
		if(columnGrs != null && !columnGrs.isEmpty()) {
			return columnGrs.get(0).toString();
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
		if(columnGrs != null && !columnGrs.isEmpty()) {
			List<Object> values = columnGrs.getAllValues();
			List<String> strValues = new Vector<>();
			for(Object obj : values) {
				strValues.add(obj.toString());
			}
			return strValues;
		}

		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<>();
		for(Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
	}

	private double getDefaultValue() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if(columnGrs != null) {
			List<Object> columns = columnGrs.getAllNumericColumns();
			if(!columns.isEmpty()) {
				return ((Number) columns.get(0)).doubleValue();
			}
		}
		
		// else, we assume it is column values in the curRow
		List<Object> columns = this.curRow.getAllNumericColumns();
		if(!columns.isEmpty()) {
			return ((Number) columns.get(0)).doubleValue();
		}
		
		// by default we return 0
		return 0.0;
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if(columnGrs != null && !columnGrs.isEmpty()) {
			return columnGrs.get(0).toString();
		}
		return null;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(X_COLUMNS)) {
			return "x variable input";
		} else if (key.equals(Y_COLUMN)) {
			return "y variable input";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}

/**
 * Need an inner class that extends OLS from Apache Commons to get some of the specific data out
 * since methods are protected and not public
 */
class OLSCalculator extends OLSMultipleLinearRegression {
	
	public double[] getCoefArray() {
		return calculateBeta().toArray();
	}
	
	public double[] getResiduals() {
		return calculateResiduals().toArray();
	}
	
	public double[] getEstimateArray() {
		return getX().operate(calculateBeta()).toArray();
	}
	
}