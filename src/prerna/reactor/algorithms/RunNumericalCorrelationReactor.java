package prerna.reactor.algorithms;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunNumericalCorrelationReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(RunNumericalCorrelationReactor.class);

	private static final String CLASS_NAME = RunNumericalCorrelationReactor.class.getName();

	public RunNumericalCorrelationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.DEFAULT_VALUE_KEY.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		// figure out inputs
		List<String> numericalCols = getColumns();
		int numCols = numericalCols.size();
		if(numCols == 0) {
			String errorString = "No columns were passed as attributes for the classification routine.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		double missingVal = getDefaultValue();

		SelectQueryStruct qs = new SelectQueryStruct();
		for(int i = 0; i < numCols; i++) {
			String header = numericalCols.get(i);
			QueryColumnSelector qsHead = new QueryColumnSelector(header);
			qs.addSelector(qsHead);
		}
		qs.mergeImplicitFilters(dataFrame.getFrameFilters());
		
		double[][] correlationData = null;
		IRawSelectWrapper it = null;
		try {
			it = dataFrame.query(qs);
			logger.info("Start iterating through data to determine correlation");
			correlationData = runCorrelation(it, numCols, missingVal, logger);
			logger.info("Done iterating through data to determine correlation");
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if (correlationData == null) {
			throw new NullPointerException("correlationData cannot be null here.");
		}

		List<Object[]> data = new Vector<>();
		for(int i = 0; i < numCols; i++) {
			String numColName = numericalCols.get(i);
			for(int j = 0; j < numCols; j++) {
				Object[] dataRow = new Object[3];
				dataRow[0] = numColName;
				dataRow[1] = numericalCols.get(j);
				dataRow[2] = correlationData[i][j];
				data.add(dataRow);
			}
		}
		
		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getHeatMapData(getPanelId(), "Column Header X", "Column Header Y", "Correlation", data);
		this.insight.getTaskStore().addTask(taskData);
		
		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "NumericalCorrelation");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"NumericalCorrelation", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// now return this object
		// we are returning the name of our table that sits in R; it is structured as a list of entries: x,y,cor
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

	private double[][] runCorrelation(
			Iterator<IHeadersDataRow> it, 
			int numHeaders, 
			double defaultVal,
			Logger logger) {
		// sum of x
		double[] sx = new double[numHeaders];
		// sum of x^2
		double[] sxx = new double[numHeaders];
		// sum of x * y
		double[][] sxy = new double[numHeaders][numHeaders];
		
		int n = 0;
		while(it.hasNext()) {
			Object[] row = it.next().getValues();
			// get a clean version so we can return this for view
			double[] doubleRow = new double[numHeaders];
			
			// i want to compare all the values together
			// value compared to itself is 1
			// so i can just ignore that
			for(int i = 0; i < numHeaders; i++) {
				double x = getDouble(row[i], defaultVal);
				// add value to sum of this column
				sx[i] += x;
				// add square of this value to sum of this column squared
				sxx[i] += x*x;
				
				// only need to do half of this
				for(int j = i+1; j < numHeaders; j++) {
					double y = getDouble(row[j], defaultVal);
					// add product of this column to every other column;
					sxy[i][j] += x*y;
					sxy[j][i] = sxy[i][j];
				}

				// dont forget we need to store this value
				doubleRow[i] = x;
			}
			
			// logging
			if(n % 100 == 0) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Finished row number = " + n);
				Configurator.setLevel(logger.getName(), Level.OFF);
			}
			// we are done with this row
			n++;
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
		
		double[][] retMatrix = new double[numHeaders][numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			for(int j = i; j < numHeaders; j++) {
				if(i == j) {
					retMatrix[i][j] = 1;
					retMatrix[j][i] = 1;
				} else {
					double cov = sxy[i][j] / n - sx[i] * sx[j] / n / n;
					double sigmax = Math.sqrt(sxx[i] / n - sx[i] * sx[i] / n / n );
					double sigmay = Math.sqrt(sxx[j] / n - sx[j] * sx[j] / n / n );
					
					retMatrix[i][j] = cov / sigmax / sigmay;
					retMatrix[j][i] = retMatrix[i][j];
				}
			}
		}
		
		return retMatrix;
	}
	
	private double getDouble(Object obj, double defaultVal) {
		if(obj instanceof Number) {
			return ((Number) obj).doubleValue();
		}
		return defaultVal;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	
	/*
	 * Retrieving inputs
	 */
	
	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[0]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
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
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
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
		
		// we return 0 by default
		return 0.0;
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if(columnGrs != null && !columnGrs.isEmpty()) {
			return columnGrs.get(0).toString();
		}
		return null;
	}
}
