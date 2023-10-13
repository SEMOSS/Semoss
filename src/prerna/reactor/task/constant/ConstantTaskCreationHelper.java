package prerna.reactor.task.constant;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;

public class ConstantTaskCreationHelper {

	private ConstantTaskCreationHelper() {
		
	}
	
    /*
     * Below methods are for sending data to the FE as ConstantDataTask's
     * This is because the FE needs certain information to know how to paint
     * ^i.e. task options, etc.
     * So creating wrappers to facilitate that
     */
    
	/**
	 * This method allows us to create a bar chart from the data values object[][] created with the breaks and counts
	 * @param label
	 * @param value - for the histogram this is "Frequency"
	 * @param dataValues - this better be a List<Object[]> or Object[][] since the serialization to JSON is the same
	 */
	public static ConstantDataTask getBarChartInfo(String panelId, String label, String value, Object dataValues) {
		// create the weird object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);
		returnData.put("headers", new String[]{label, value});
		task.setOutputData(returnData);
		
		//create maps to set the task options
		//main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();
		
		//this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>(); 
		keyMap.put("layout", "Column");
		
		//within keyMap, we need a map to store the maps that comprise alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		Vector<String> labelList = new Vector<String>();
		labelList.add(label);
		alignmentMap.put("label",  labelList);
		Vector<String> valueList = new Vector<String>();
		valueList.add(value);
		alignmentMap.put("value", valueList);
		alignmentMap.put("tooltip", "[]");
		
		keyMap.put("alignment", alignmentMap);
		
		mapOptions.put(panelId, keyMap);
		//the final mapping looks like this:
		//taskOptions={0={layout=Column, alignment={tooltip=[], labels=[col1], value=[col2]}}}
		
		//set task options
		task.setTaskOptions(new TaskOptions(mapOptions));

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
		
		return task;
	}
	
	/**
	 * This method allows us to create a grid view from the data values object[][] created with the breaks and counts
	 * @param panelId - panel id
	 * @param labels - column names of data table
	 * @param dataValues - this better be a List<Object[]> or Object[][] since the serialization to JSON is the same
	 */
	public static ConstantDataTask getGridData(String panelId, String[] labels, Object dataValues) {
		// create the weird object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);
		returnData.put("headers", labels);
		task.setOutputData(returnData);
		
		//create maps to set the task options
		//main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();
		
		//this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>(); 
		keyMap.put("layout", "Grid");
		
		//within keyMap, we need a map to store the maps that comprise alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		alignmentMap.put("label", labels);
		keyMap.put("alignment", alignmentMap);
		
		mapOptions.put(panelId, keyMap);
		//the final mapping looks like this:
		//taskOptions={0={layout=GRID, alignment={label=[Col1, Col2, Col3]}}}
		
		//set task options
		task.setTaskOptions(new TaskOptions(mapOptions));

		List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
		for(int i = 0; i < labels.length; i++) {
			Map<String, Object> labelMap = new Hashtable<String, Object>();
			labelMap.put("header", labels[i]);
			labelMap.put("alias", labels[i]);
			labelMap.put("derived", true);
			labelMap.put("type", "STRING");
			vizHeaders.add(labelMap);
		}
		task.setHeaderInfo(vizHeaders);

		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		task.setFormatMap(formatMap);
		
		return task;
	}
	
	/**
	 * This method allows us to create a heatmap from the data values object[][] created with the breaks and counts
	 * @param panelId - panel id
	 * @param x - x axis
	 * @param y - y axis
	 * @param heat - heat for heat map
	 * @param dataValues - this better be a List<Object[]> or Object[][] since the serialization to JSON is the same
	 */
	public static ConstantDataTask getHeatMapData(String panelId, String x, String y, String heat, Object dataValues) {

		// create the object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);

		String[] labels = new String[] { x, y, heat };

		returnData.put("headers", labels);
		task.setOutputData(returnData);

		// create maps to set the task options
		// main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();

		// this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>();
		keyMap.put("layout", "HeatMap");

		// within keyMap, we need a map to store the maps that comprise
		// alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		alignmentMap.put("x", new Object[]{x});
		alignmentMap.put("y", new Object[]{y});
		alignmentMap.put("heat", new Object[]{heat});
		keyMap.put("alignment", alignmentMap);

		mapOptions.put(panelId, keyMap);
		// the final mapping looks like this:
		// taskOptions={0={layout=Heat, alignment={label=[x, y, heat]}}}

		// set task options
		task.setTaskOptions(new TaskOptions(mapOptions));

		List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
		for (int i = 0; i < labels.length; i++) {
			Map<String, Object> labelMap = new Hashtable<String, Object>();
			labelMap.put("header", labels[i]);
			labelMap.put("alias", labels[i]);
			labelMap.put("derived", true);
			labelMap.put("type", "STRING");
			vizHeaders.add(labelMap);
		}
		task.setHeaderInfo(vizHeaders);

		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		task.setFormatMap(formatMap);

		return task;
	}
	
	
	/**
	 * This method allows us to create a scatterplot from the data values object[][] created with the breaks and counts
	 * @param panelId - panel id
	 * @param x - x axis
	 * @param y - y axis
	 * @param heat - heat for heat map
	 * @param dataValues - this better be a List<Object[]> or Object[][] since the serialization to JSON is the same
	 */
	public static ConstantDataTask getScatterPlotData(String panelId, String label, String x, String y, Object dataValues) {

		// create the object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);

		String[] labels = new String[] {label, x, y};

		returnData.put("headers", labels);
		task.setOutputData(returnData);

		// create maps to set the task options
		// main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();

		// this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>();
		keyMap.put("layout", "Scatter");

		// within keyMap, we need a map to store the maps that comprise
		// alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		alignmentMap.put("label", new Object[]{label});
		alignmentMap.put("x", new Object[]{x});
		alignmentMap.put("y", new Object[]{y});
		keyMap.put("alignment", alignmentMap);

		mapOptions.put(panelId, keyMap);

		// set task options
		task.setTaskOptions(new TaskOptions(mapOptions));

		List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
		for (int i = 0; i < labels.length; i++) {
			Map<String, Object> labelMap = new Hashtable<String, Object>();
			labelMap.put("header", labels[i]);
			labelMap.put("alias", labels[i]);
			labelMap.put("derived", true);
			labelMap.put("type", "NUMBER");
			vizHeaders.add(labelMap);
		}
		task.setHeaderInfo(vizHeaders);

		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		task.setFormatMap(formatMap);

		return task;
	}
	
	/**
	 * This method allows us to create a grid view from the data values object[][] created with the breaks and counts
	 * @param panelId - panel id
	 * @param x - x axis
	 * @param y - y axis
	 * @param heat - heat for heat map
	 * @param dataValues - this better be a List<Object[]> or Object[][] since the serialization to JSON is the same
	 */
	public static ConstantDataTask getScatterPlotData(String panelId, String[] labels, Object dataValues, String series, 
			String x, String y, String z, String label, String[] facet) {
		// create the weird object the FE needs to paint a bar chart
		ConstantDataTask task = new ConstantDataTask();
		task.setId("TEMP_ID");
		Map<String, Object> returnData = new Hashtable<String, Object>();  //returnData makes up the "data" parent element 
		returnData.put("values", dataValues);
		returnData.put("headers", labels);
		task.setOutputData(returnData);

		// create maps to set the task options
		// main map that will be passed into the setTaskOptions method
		Map<String, Object> mapOptions = new HashMap<String, Object>();

		// this map (keyMap) comprises the mapping of both layout and alignment
		Map<String, Object> keyMap = new HashMap<String, Object>();
		keyMap.put("layout", "Scatter");

		// within keyMap, we need a map to store the maps that comprise alignment
		Map<String, Object> alignmentMap = new HashMap<String, Object>();
		String[] xArray = new String[] {x};
		alignmentMap.put("x",xArray);
		String[] yArray = new String[] {y};
		alignmentMap.put("y", yArray);
		String[] labelArray = new String[] {label};
		alignmentMap.put("label", labelArray);
		//size is optional
		if (z == null || z.isEmpty()) {
			alignmentMap.put("z", new String[]{});
		} else {
			String[] zArray = new String[] {z};
			alignmentMap.put("z", zArray);
		}
		//color is optional
		if (series == null || series.isEmpty()) {
			alignmentMap.put("series", new String[]{});
		} else {
			String[] seriesArray = new String[] {series};
			alignmentMap.put("series", seriesArray);
		}
		//facet is optional
		if (facet != null){
			if (facet.length > 0) alignmentMap.put("facet", facet);
		} else {
			alignmentMap.put("facet", new String[]{});
		}
		keyMap.put("alignment", alignmentMap);

		mapOptions.put(panelId, keyMap);
		// the final mapping looks like this:
		// taskOptions={0={layout=GRID, alignment={label=[Col1, Col2, Col3]}}}

		// set task options
		task.setTaskOptions(new TaskOptions(mapOptions));

		List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
		for (int i = 0; i < labels.length; i++) {
			Map<String, Object> labelMap = new Hashtable<String, Object>();
			labelMap.put("header", labels[i]);
			labelMap.put("alias", labels[i]);
			labelMap.put("derived", true);
			labelMap.put("type", "STRING");
			vizHeaders.add(labelMap);
		}
		task.setHeaderInfo(vizHeaders);

		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		task.setFormatMap(formatMap);

		return task;
	}
	
}
