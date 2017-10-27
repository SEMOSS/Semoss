package prerna.ds.r;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RSingleton;
import prerna.util.ArrayUtilityMethods;

public class RBuilder extends AbstractRBuilder {

	// holds the connection for RDataFrame to the instance of R running
	private RConnection retCon;
	private String port;
	
	public RConnection getConnection() {
		return this.retCon;
	}
	
	public String getPort() {
		return this.port;
	}
	
	public RBuilder() throws RserveException {
		this.retCon = RSingleton.getConnection();
//		this.port = Utility.findOpenPort();
//		this.logger.info("Starting it on port.. " + port);
//		// need to find a way to get a common name
//		masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
//		this.retCon = new RConnection("127.0.0.1", Integer.parseInt(port));

		// load required libraries
		loadDefaultLibraries();		
	}

	public RBuilder(String dataTableName) throws RserveException {
		this();
		if(dataTableName != null && !dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
		}
	}

	public RBuilder(String dataTableName, RConnection retCon, String port) throws RserveException {
		this.retCon = retCon;
		this.port = port;
		// load required libraries
		loadDefaultLibraries();
		if(dataTableName != null && !dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
		}
	}

	private void loadDefaultLibraries() throws RserveException {
		// load all the libraries
		this.logger.info("TRYING TO LOAD PACAKGE: splitstackshape");
		this.retCon.eval("library(splitstackshape);");
		this.logger.info("SUCCESS!");
		// data table
		this.logger.info("TRYING TO LOAD PACAKGE: data.table");
		this.retCon.eval("library(data.table);");
		this.logger.info("SUCCESS!");
		// xlsx
		this.logger.info("TRYING TO LOAD PACAKGE: xlsx");
		this.retCon.eval("library(xlsx);");
		this.logger.info("SUCCESS!");
		// reshape2
		this.logger.info("TRYING TO LOAD PACAKGE: reshape2");
		this.retCon.eval("library(reshape2);");
		this.logger.info("SUCCESS!");
		// rjdbc
		this.logger.info("TRYING TO LOAD PACAKGE: RJDBC");
		this.retCon.eval("library(RJDBC);");
		this.logger.info("SUCCESS!");
		// stringr
		this.logger.info("TRYING TO LOAD PACAKGE: stringr");
		this.retCon.eval("library(stringr);");
		this.logger.info("SUCCESS!");
	}
	
	@Override
	protected void evalR(String r) {
		executeR(r);
	}
	
	protected REXP executeR(String r) {
		try {
			REXP result = retCon.parseAndEval(r);
			return result;
		} catch (REngineException | REXPMismatchException e) {
			e.printStackTrace();
			String errorMessage = null;
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				errorMessage = e.getMessage();
			} else {
				errorMessage = "Unexpected error in execution of R routine ::: " + r;
			}
			throw new IllegalArgumentException(errorMessage);
		}
	}
	
	/**
	 * Wrap the R script in a try-eval in order to get the same error message that a user would see if using
	 * the R console
	 * @param rscript			The R script to execute
	 * @return					The R script wrapped in a try-eval statement
	 */
	protected String addTryEvalToScript(String rscript) {
		return "try(eval(" + rscript + "), silent=FALSE)";
	}
	
	/**
	 * Given the try-error syntax surrounding the r script to execute
	 * This seems if the result contains it to grab the same error message that R shows
	 * upon execution
	 * @param result				The R object result which contains the try-eval
	 * @param e						The exception to throw for  debugging
	 * @param defaultMessage		The default error message to show in case the 
	 * 									try-eval didn't have an error response
	 */
	protected void handleRException(REXP result, Exception e, String defaultMessage) {
		e.printStackTrace();
		if(result != null && result.inherits("try-error")) {
			try {
				defaultMessage = result.asString();
			} catch (REXPMismatchException e1) {
				e1.printStackTrace();
			}
		}
		throw new IllegalArgumentException(defaultMessage);
	}
	
	/**
	 * Calculate the number of rows in the data table
	 * @return
	 */
	protected int getNumRows() {
		return getNumRows(this.dataTableName);
	}
	
	/**
	 * Calculate the number of rows in the data table
	 * @return
	 */
	protected int getNumRows(String varName) {
		REXP result = executeR( addTryEvalToScript( "nrow(" + varName + ")"  ) );
		int numRows = 0;
		try {
			numRows = result.asInteger();
		} catch (REXPMismatchException e) {
			handleRException(result, e, "Error in calculating the number of rows for the datatable");
		}
		return numRows;
	}
	
	/**
	 * Determine if the datatable is empty
	 * @return
	 */
	public boolean isEmpty() {
		//TO DO - test to determine need for quotes around dataTableName
		REXP result = executeR( addTryEvalToScript( "exists(\'" + this.dataTableName + "\')" ) );
		try {
			// we get the boolean expression as an integer
			// 1 = TRUE, 0 = FALSE
			int intBooleanVal = result.asInteger();
			if(intBooleanVal == 1) {
				// so it exists, but we should probably make sure it has at least one row
				if(getNumRows() > 0) {
					return false;
				}
			}
		} catch (REXPMismatchException e) {
			try {
				if(result.asString().contains("object 'datatable' not found")) {
					return true;
				}
			} catch (REXPMismatchException e1) {
				// just handle the first exception that was thrown
				handleRException(result, e, "Couldn't check if datatable exists");
			}
			handleRException(result, e, "Couldn't check if datatable exists");
		}
		
		return true;
	}
	
	@Override
	public String[] getColumnNames() {
		return getColumnNames(this.dataTableName);
	}

	@Override
	public String[] getColumnTypes() {
		return getColumnTypes(this.dataTableName);
	}

	@Override
	public String[] getColumnNames(String varName) {
		REXP namesR = executeR("names(" + varName + ")");
		String[] names = null;
		try {
			names = namesR.asStrings();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return names;
	}

	@Override
	public String[] getColumnTypes(String varName) {
		REXP typesR = executeR("sapply(" + varName + " , class)");
		String[] types = null;
		try {
			types = typesR.asStrings();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return types;
	}
	
	
	@Override
	protected Object[] getDataRow(String rScript, String[] headerOrdering) {
		REXP rs = executeR(rScript);
		Object[] retArray = null;
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		if(result instanceof Map) {
			retArray =  processMapReturn((Map<String, Object>) result, headerOrdering).get(0);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			retArray = (Object[]) processListReturn((List) result, headerOrdering, returnNames).get(0);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
		
		return retArray;
	}


	@Override
	protected List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = executeR(rScript);
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		if(result instanceof Map) {
			return processMapReturn((Map<String, Object>) result, headerOrdering);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			return processListReturn((List) result, headerOrdering, returnNames);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
	}

	private List<Object[]> processListReturn(List<Object[]> result, String[] headerOrdering, String[] returnNames) {
		List<Object[]> retArr = new Vector<Object[]>(500);

		// match the returns based on index
		int numHeaders = headerOrdering.length;
		int[] headerIndex = new int[numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			headerIndex[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(returnNames, headerOrdering[i]);
		}
		
		for(int i = 0; i < numHeaders; i++) {
			// grab the right column index
			int columnIndex = headerIndex[i];
			// each column comes back as an array
			// need to first initize my return matrix
			Object col = result.get(columnIndex);
			if(col instanceof Object[]) {
				Object[] columnResults = (Object[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof double[]) {
				double[] columnResults = (double[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof int[]) {
				int[] columnResults = (int[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			}
		}
		
		return retArr;
	}

	private List<Object[]> processMapReturn(Map<String, Object> result,  String[] headerOrdering) {
		List<Object[]> retArr = new Vector<Object[]>(500);
		int numColumns = headerOrdering.length;
		for(int idx = 0; idx < numColumns; idx++) {
			Object val = result.get(headerOrdering[idx]);

			if(val instanceof Object[]) {
				Object[] data = (Object[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof double[]) {
				double[] data = (double[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof int[]) {
				int[] data = (int[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if (val instanceof String) {
				String data = (String) val;
				if (retArr.size() == 0) {
					Object[] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values[idx] = data;
				}
			} else if (val instanceof Double) {
				Double data = (Double) val;
				if (retArr.size() == 0) {
					Object [] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values [idx] = data;
				}	
			} else if (val instanceof Integer){
				Integer data = (Integer) val;
				if (retArr.size() == 0) {
					Object [] values = new Object [numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object [] values = retArr.get(0);
					values [idx] = data;
				}
			} else {
				logger.info("ERROR ::: Could not identify the return type for this iterator!!!");
			}
		}
		return retArr;
	}
	
	@Override
	public Object getScalarReturn(String rScript) {
		REXP rs = executeR(rScript);
		Object val = null;
		try {
			Map<String, Object> result = (Map<String, Object>) rs.asNativeJavaObject();
			// we assume there is only 1 value to return
			Object value = result.get(result.keySet().iterator().next());
			if(value instanceof Object[]) {
				val = ((Object[]) value)[0];
			} else if(value instanceof double[]) {
				val = ((double[]) value)[0];
			} else if( value instanceof int[]) {
				val = ((int[]) value)[0];
			} else {
				val = value;
			}
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return val;
	}
}
