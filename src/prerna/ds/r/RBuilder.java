package prerna.ds.r;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RSingleton;
import prerna.test.TestUtilityMethods;
import prerna.util.Utility;

public class RBuilder extends AbstractRBuilder {

	private static final Logger LOGGER = LogManager.getLogger(RBuilder.class.getName());

//	private static final String CREATE_DATA_TABLE_METHOD = "createEmptyDataTable.123456";
//	private static final String ADD_ROW_TO_DATA_TABLE_METHOD = "appendToDataTable.123456";
//	private static final String REMOVE_EMPTY_ROWS = "removeEmptyRows.123456";

	// holds the connection for RDataFrame to the instance of R running
	private RConnection retCon;
	private String port;
	private String dataTableName = "datatable";
	
	public RConnection getConnection() {
		return this.retCon;
	}
	
	public String getPort() {
		return this.port;
	}
	
	public RBuilder() throws RserveException {
		RConnection masterCon = RSingleton.getConnection();
		this.port = Utility.findOpenPort();
		LOGGER.info("Starting it on port.. " + port);
		// need to find a way to get a common name
		masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
		this.retCon = new RConnection("127.0.0.1", Integer.parseInt(port));

		// load required libraries
		loadDefaultLibraries();		
	}

	public RBuilder(String dataTableName) throws RserveException {
		this();
		if(this.dataTableName != null && !this.dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
		}
	}

	public RBuilder(String dataTableName, RConnection retCon, String port) throws RserveException {
		this.retCon = retCon;
		this.port = port;
		// load required libraries
		loadDefaultLibraries();
		if(this.dataTableName != null && !this.dataTableName.trim().isEmpty()) {
			this.dataTableName = dataTableName;
		}
	}

	private void loadDefaultLibraries() throws RserveException {
		// load in the data.table package
		this.retCon.eval("library(data.table)");
		// load in the sqldf package to run sql queries
		this.retCon.eval("library(sqldf)");
		// load all the libraries
		this.retCon.eval("library(splitstackshape);");
		// data table
		this.retCon.eval("library(data.table);");
		// reshape2
		this.retCon.eval("library(reshape2);");
		// rjdbc
		this.retCon.eval("library(RJDBC);");
	}
	
	protected String getTableName() {
		return this.dataTableName;
	}
	
	protected Double executeStat(String colName, String statRoutine) {
		Double val = null;
		REXP result = null;
		try {
			result = retCon.parseAndEval( addTryEvalToScript( statRoutine + "(" + this.dataTableName + "[,c(\"" + colName + "\")])") );
			val = result.asDouble();
		} catch (REXPMismatchException | REngineException e) {
			String defaultError = "Unexpected error in calculation of max for column = " + colName;
			handleRException(result, e, defaultError);
		}

		return val;
	}
	
	protected String getROutput(String rScript) {
		String newScript = "try(eval( paste(capture.output(print(" + rScript + ")),collapse='\n') ), silent=FALSE)";
		try {
			REXP result = retCon.parseAndEval(newScript);
			return result.asString();
		} catch (REngineException | REXPMismatchException e) {
			e.printStackTrace();
			String errorMessage = null;
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				errorMessage = e.getMessage();
			} else {
				errorMessage = "Unexpected error in execution of R routine ::: " + rScript;
			}
			throw new IllegalArgumentException(errorMessage);
		}
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
	
	
	protected Iterator<Object[]> iterator(String[] headerNames, int limit, int offset) {
		return new RIterator(this, headerNames, limit, offset);
	}
	
	/**
	 * Determine if the datatable is empty
	 * @return
	 */
	public boolean isEmpty() {
		REXP result = executeR( addTryEvalToScript( "exists(" + this.dataTableName + ")" ) );
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
		Object[] retArray = null;;
		try {
			Map<String, Object> result = (Map<String, Object>) rs.asNativeJavaObject();
			
			int retSize = headerOrdering.length;
			retArray = new Object[retSize];
			for(int colIndex = 0; colIndex < retSize; colIndex++) {
				Object val = result.get(headerOrdering[colIndex]);
				if(val instanceof String) {
					retArray[colIndex] = val;
				} else if(val instanceof Object[]) {
					retArray[colIndex] = ((Object[]) val)[0];
				} else if(val instanceof double[]) {
					retArray[colIndex] = ((double[]) val)[0];
				} else if(val instanceof int[]) {
					retArray[colIndex] = ((int[]) val)[0];
				} else {
					retArray[colIndex] = val;
				}
			}
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		
		return retArray;
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

	
	
	
	
	
	
	///////////////////////////////// 
	/////////////////////////////////
	// random testing
	
	public static void main(String[] args) throws RserveException {
		
		TestUtilityMethods.loadDIHelper();
		
		RBuilder builder = new RBuilder();
		builder.executeR("setwd(\"C:/Users/mahkhalil/Desktop\");");
		builder.executeR("datatable <- read.csv(\"Movie Data.csv\");");
		Double val =  null;
		try {
			val = builder.executeStat("Genre", "max");
		} catch(Exception e) {
			System.out.println(">>>>>>>>>>>>>>>>> " + e.getMessage());
		}
		val = builder.executeStat("MovieBudget", "min");
		System.out.println(">>>>>>>>>>>>>>>>> " + val);
		val = builder.executeStat("MovieBudget", "max");
		System.out.println(">>>>>>>>>>>>>>>>> " + val);

		REXP val2 = builder.executeR(builder.addTryEvalToScript("a<-createEmptyDataTable.123456(c(1,2,3,4,5))"));
		try {
			System.out.println(val2.asNativeJavaObject());
			
			RList a = val2.asList();
			ListIterator it = a.listIterator();
			while(it.hasNext()) {
				Object obj = it.next();
				System.out.println(obj);
			}
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
