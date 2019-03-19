package prerna.sablecc2.reactor.frame.py.analytics;


import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class PyNumericalCorrelationReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = PyNumericalCorrelationReactor.class.getName();
	
	public PyNumericalCorrelationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.DEFAULT_VALUE_KEY.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		PandasFrame frame = (PandasFrame)dataFrame;
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		
		// figure out inputs
		// need the panelId for passing the task to the FE
		String panelId = getPanelId();

		List<String> numericalCols = getColumns();
		int numCols = numericalCols.size();
		if(numCols == 0) {
			String errorString = "No columns were passed as attributes for the correlation routine.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		//double missingVal = getDefaultValue();

		// need the headers as a list of strings
		StringBuilder columns = new StringBuilder("list([");
		
		for(int columnIndex = 0; columnIndex < numCols; columnIndex++) {
			String header = numericalCols.get(columnIndex);
			if(header.contains("__")) {
				String[] split = header.split("__");
				header = split[1];
			} 
			if(columnIndex == 0)
				columns.append("'").append(header).append("'");
			else
				columns.append(",").append("'").append(header).append("'");

		}
		columns.append("])");
		
		// get the correlation data from the run r correlation algorithm
		logger.info("Start iterating through data to determine correlation");
		String correlationDataTable = Utility.getRandomString(6);
		frame.runScript(correlationDataTable + " = " + frameName + "w.get_correlation(" + columns +")");
		logger.info("Done iterating through data to determine correlation");
		
		// create the object to return to the FE
		// the length of the object will be numCols^2
		// there will always be three rows x,y,cor
		int length = numCols * numCols;
		Object[][] retOutput = new Object[length][3];
		
		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] heatMapHeaders = new String[]{"Column_Header_X", "Column_Header_Y", "Correlation"};
		String query = correlationDataTable + "[" + 1 + ":" + length + "]";
		List bulkRow = (List)frame.runScript(correlationDataTable + ".values.tolist()");
		// each entry into the list is a row - we need to put this in the form of Object[][]
		for (int i = 0; i < bulkRow.size(); i++) {
			ArrayList oneRow = (ArrayList)bulkRow.get(i);
			retOutput[i] = new String[oneRow.size()];
			oneRow.toArray(retOutput);
		}
		
		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getHeatMapData(panelId, "Column_Header_X", "Column_Header_Y", "Correlation", retOutput);
		this.insight.getTaskStore().addTask(taskData);

		// variable cleanup
		frame.runScript("del " + correlationDataTable);

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
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(
				new NounMetadata("Numerical Correlation ran successfully!", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
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
