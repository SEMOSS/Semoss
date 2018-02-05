package prerna.sablecc2.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ga.GATracker;

public class NumericalCorrelationReactor extends AbstractReactor {

	private static final String CLASS_NAME = NumericalCorrelationReactor.class.getName();
	
	public NumericalCorrelationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.DEFAULT_VALUE_KEY.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
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

		// I need to return back headers
		// and the actual data rows
		// and a dataTableAlign object
		// in addition to the specific correlation data
		String[] retHeaders = new String[numCols];
		List<double[]> rowData = new ArrayList<double[]>();;
		Map<String, String> dataTableAlign = new HashMap<String, String>();
		
		QueryStruct2 qs = new QueryStruct2();
		for(int i = 0; i < numCols; i++) {
			String header = numericalCols.get(i);
			QueryColumnSelector qsHead = new QueryColumnSelector();
			if(header.contains("__")) {
				String[] split = header.split("__");
				qsHead.setTable(split[0]);
				qsHead.setColumn(split[1]);
				retHeaders[i] = split[1];
			} else {
				qsHead.setTable(header);
				retHeaders[i] = header;
			}
			dataTableAlign.put("dim " + i, retHeaders[i]);
			qs.addSelector(qsHead);
		}
		qs.mergeFilters(dataFrame.getFrameFilters());
		
		Iterator<IHeadersDataRow> it = dataFrame.query(qs);
		logger.info("Start iterating through data to determine correlation");
		double[][] correlationData = runCorrelation(it, rowData, numCols, missingVal, logger);
		logger.info("Done iterating through data to determine correlation");

		// mock the data output
		Map<String, Object> vizData = new HashMap<String, Object>();
		vizData.put("data", rowData);
		vizData.put("headers", retHeaders);
		vizData.put("layout", "ScatterplotMatrix");
		vizData.put("panelId", getPanelId());
		vizData.put("dataTableAlign", dataTableAlign);
		// finally, i send the correlation data
		Map<String, double[][]> correlationMap = new HashMap<String, double[][]>();
		correlationMap.put("correlations", correlationData);
		vizData.put("specificData", correlationMap);
		
		// track GA data
		GATracker.getInstance().trackAnalyticsPixel(this.insight, "NumericalCorrelation");
		
		// now return this object
		return new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
	}

	private double[][] runCorrelation(
			Iterator<IHeadersDataRow> it, 
			List<double[]> rowData, 
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
				logger.setLevel(Level.INFO);
				logger.info("Finished row number = " + n);
				logger.setLevel(Level.OFF);
			}
			// we are done with this row
			n++;
			// store it as well
			rowData.add(doubleRow);
		}
		logger.setLevel(Level.INFO);
		
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
				List<String> strValues = new Vector<String>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
		
		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
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
			if(columns.size() > 0) {
				return ((Number) columns.get(0)).doubleValue();
			}
		}
		
		// else, we assume it is column values in the curRow
		List<Object> columns = this.curRow.getAllNumericColumns();
		if(columns.size() > 0) {
			return ((Number) columns.get(0)).doubleValue();
		}
		
		// return 0 by default;
		return 0.0;
	}
	
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
}
