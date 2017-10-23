package prerna.sablecc2.reactor.frame.r;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;

public abstract class AbstractRFrameReactor extends AbstractFrameReactor {

	protected AbstractRJavaTranslator rJavaTranslator;

	/**
	 * This method must be called to initialize the rJavaTranslator
	 */
	protected void init() {
		this.rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		this.rJavaTranslator.startR(); 
	}

	/**
	 * This method is used to recreate the frame metadata
	 * when we execute a script that modifies the data structure
	 * @param frameName
	 */
	protected void recreateMetadata(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(frameName);
		String[] colTypes = getColumnTypes(frameName);
		RDataTable newTable = new RDataTable(frameName);
		ImportUtility.parseColumnsAndTypesToFlatTable(newTable, colNames, colTypes, frameName);
		this.insight.setDataMaker(newTable);
	}

	/**
	 * This method is used to fix the frame headers to be valid 
	 * @param frameName
	 * @param newColName
	 */
	protected String getCleanNewHeader(String frameName, String newColName) {
		// make the new column name valid
		HeadersException headerChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColumns(frameName);
		String validNewHeader = headerChecker.recursivelyFixHeaders(newColName, currentColumnNames);
		return validNewHeader;
	}

	/**
	 * This method is used to get the column names of a frame
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.rJavaTranslator.getColumns(frameName);
	}

	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		return this.rJavaTranslator.getColumnTypes(frameName);
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		return this.rJavaTranslator.getColumnType(frameName, column);
	}
	
	/**
	 * Change the frame column type
	 * @param frame
	 * @param frameName
	 * @param colName
	 * @param newType
	 * @param dateFormat
	 */
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat) {
		this.rJavaTranslator.changeColumnType(frame, frameName, colName, newType, dateFormat);
	}
	
	/**
	 * This method allows us to create a bar chart from the data values object[][] created with the breaks and counts
	 * @param label
	 * @param value - for the histogram this is "Frequency"
	 * @param dataValues
	 */
	public Map<String, Object> getBarChartInfo(String panelId, String label, String value, Object[][] dataValues) {
		// create the weird object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);
		returnData.put("headers", new String[]{label, value});
		task.setOutputObject(returnData);
		
		//create maps to set the task options
		//main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();
		
		//this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>(); 
		keyMap.put("layout", "Column");
		
		//within keyMap, we need a map to store the maps that comprise alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		alignmentMap.put("label", "[" + label + "]");
		alignmentMap.put("value", "[" + value + "]");
		alignmentMap.put("tooltip", "[]");
		
		keyMap.put("alignment", alignmentMap);
		
		mapOptions.put(panelId, keyMap);
		//the final mapping looks like this:
		//taskOptions={0={layout=Column, alignment={tooltip=[], label=[MovieBudget], value=[Frequency]}}}
		
		//set task options
		task.setTaskOptions(mapOptions);

		List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
		Map<String, Object> labelMap = new Hashtable<String, Object>();
		labelMap.put("header", label);
		labelMap.put("derived", true);
		labelMap.put("alias", label);

		Map<String, Object> frequencyMap = new Hashtable<String, Object>();
		frequencyMap.put("header", value);
		frequencyMap.put("derived", true);
		frequencyMap.put("alias", value);

		vizHeaders.add(labelMap);
		vizHeaders.add(frequencyMap);

		task.setHeaderInfo(vizHeaders);

		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		task.setFormatMap(formatMap);
		
		return task.collect(0, true);
	}
}
