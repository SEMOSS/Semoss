package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.Rserve.RConnection;

import prerna.ds.r.RDataTable;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.DIHelper;

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
		//clean headers
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + frameName + ") <- c(" + rColNames + ")";
		this.rJavaTranslator.executeEmptyR(script);
		RDataTable newTable = null;
		if (retrieveVariable(IRJavaTranslator.R_CONN) != null && retrieveVariable(IRJavaTranslator.R_PORT) != null) {
			newTable = new RDataTable(frameName, (RConnection) retrieveVariable(IRJavaTranslator.R_CONN), (String) retrieveVariable(IRJavaTranslator.R_PORT));
		} else {
			newTable = new RDataTable(frameName);
		}
		ImportUtility.parseColumnsAndTypesToFlatTable(newTable, colNames, colTypes, frameName);
		this.insight.setDataMaker(newTable);
	}
	
	/**
	 * Renames columns based on a string[] of old names and a string[] of new
	 * names Used by synchronize methods
	 * 
	 * @param frameName
	 * @param oldNames
	 * @param newNames
	 *            boolean print
	 */
	protected void renameColumn(String frameName, String[] oldNames, String[] newNames, boolean print) {
		int size = oldNames.length;
		if (size != newNames.length) {
			throw new IllegalArgumentException("Names arrays do not match in length");
		}
		StringBuilder oldC = new StringBuilder("c(");
		int i = 0;
		oldC.append("'").append(oldNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			oldC.append(", '").append(oldNames[i]).append("'");
		}
		oldC.append(")");

		StringBuilder newC = new StringBuilder("c(");
		i = 0;
		newC.append("'").append(newNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			newC.append(", '").append(newNames[i]).append("'");
		}
		newC.append(")");

		String script = "setnames(" + frameName + ", old = " + oldC + ", new = " + newC + ")";
		this.rJavaTranslator.executeEmptyR(script);

		if (print) {
			System.out.println("Running script : " + script);
			System.out.println("Successfully modified old names = " + Arrays.toString(oldNames) + " to new names " + Arrays.toString(newNames));
		}

		// FE passes the column name
		// but meta will still be table __ column
		for (i = 0; i < size; i++) {
			this.getFrame().getMetaData().modifyPropertyName(frameName + "__" + oldNames[i], frameName,
					frameName + "__" + newNames[i]);
		}
		this.getFrame().syncHeaders();
	}


	/**
	 * This method is used to fix the frame headers to be valid
	 * 
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
	
	protected void storeVariable(String varName, NounMetadata noun) {
		this.insight.getVarStore().put(varName, noun);
	}

	protected Object retrieveVariable(String varName) {
		NounMetadata noun = this.insight.getVarStore().get(varName);
		if (noun == null) {
			return null;
		}
		return noun.getValue();
	}

	protected void removeVariable(String varName) {
		this.insight.getVarStore().remove(varName);
	}

	protected void endR() {
		// java.lang.System.setSecurityManager(curManager);
		// clean up other things
		this.rJavaTranslator.endR();
		if(rJavaTranslator instanceof RJavaJriTranslator) {
			removeVariable(IRJavaTranslator.R_ENGINE);
			removeVariable(IRJavaTranslator.R_PORT);
		} else {
			removeVariable(IRJavaTranslator.R_CONN);
			removeVariable(IRJavaTranslator.R_PORT);
		}
		System.out.println("R Shutdown!!");
//		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
     * Get the base folder
     * @return
     */
     protected String getBaseFolder() {
          String baseFolder = null;
          try {
              baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
          } catch (Exception ignored) {
              //logger.info("No BaseFolder detected... most likely running as test...");
          }
          return baseFolder;
     }
     
     //////////////////////////////////////////////////////////////
     
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
 	public Map<String, Object> getBarChartInfo(String panelId, String label, String value, Object dataValues) {
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
 		alignmentMap.put("label", "[" + label + "]");
 		alignmentMap.put("value", "[" + value + "]");
 		alignmentMap.put("tooltip", "[]");
 		
 		keyMap.put("alignment", alignmentMap);
 		
 		mapOptions.put(panelId, keyMap);
 		//the final mapping looks like this:
 		//taskOptions={0={layout=Column, alignment={tooltip=[], labels=[col1, col2, col3]}}}
 		
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
 		
 		return task.collect(true);
 	}
 	
 	/**
 	 * This method allows us to create a grid view from the data values object[][] created with the breaks and counts
 	 * @param panelId - panel id
 	 * @param labels - column names of data table
 	 * @param dataValues
 	 */
 	public Map<String, Object> getGridData(String panelId, String[] labels, Object dataValues) {
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
 		//taskOptions={0={layout=Column, alignment={tooltip=[], label=[MovieBudget], value=[Frequency]}}}
 		
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
 		
 		return task.collect(true);
 	}
     

}
