package prerna.ds.R;

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

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RSingleton;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RBuilder {

	private static final Logger LOGGER = LogManager.getLogger(RBuilder.class.getName());

	private static final String CREATE_DATA_TABLE_METHOD = "createEmptyDataTable.123456";
	private static final String ADD_ROW_TO_DATA_TABLE_METHOD = "appendToDataTable.123456";
	private static final String REMOVE_EMPTY_ROWS = "removeEmptyRows.123456";

	// holds the connection for RDataFrame to the instance of R running
	private RConnection retCon;

	public RBuilder() throws RserveException {
		RConnection masterCon = RSingleton.getConnection();
		String port = Utility.findOpenPort();
		LOGGER.info("Starting it on port.. " + port);
		// need to find a way to get a common name
		masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
		retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
		
		// load in the data.table package
		retCon.eval("library(data.table)");
		
		// load in the R script defining the functions we are using
		String path = (String) DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		//TODO: we need to figure out how to do all of these paths OS specific...
//		path = path.replace("\\", "\\\\");
//		path = path  + "\\\\R\\\\RDataTableScripts\\\\baseFunctions.R";
		path = path.replace("\\", "/");
		path = path  + "/R/RDataTableScripts/baseFunctions.R";

		retCon.eval("source('"+ path + "')");
	}
	
	protected Double executeStat(String colName, String statRoutine) {
		Double val = null;
		REXP result = null;
		try {
			result = retCon.parseAndEval( addTryEvalToScript( statRoutine + "(datatable[,c(\"" + colName + "\")])") );
			val = result.asDouble();
		} catch (REXPMismatchException | REngineException e) {
			String defaultError = "Unexpected error in calculation of max for column = " + colName;
			handleRException(result, e, defaultError);
		}

		return val;
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
	 * Creates a new data table from an iterator
	 * @param it					The iterator to flush into a r data table
	 * @param typesMap				The data type of each column
	 */
	protected void createTableViaIterator(Iterator<IHeadersDataRow> it, Map<String, DATA_TYPES> typesMap) {
		IMetaData.DATA_TYPES[] types = null;
		if(it.hasNext()) {
			// the first iterator row needs to be processed differently to also create
			// the data table
			IHeadersDataRow row = it.next();
			
			// need to create the types array
			String[] headers = row.getHeaders();
			types = new IMetaData.DATA_TYPES[headers.length];
			for(int i = 0; i < headers.length; i++) {
				types[i] = typesMap.get(headers[i]);
			}
			
			String r_row = createRColVec(row.getValues(), types);
			// the r function is createEmptyDataTable.123456
			// it takes in the first list of elements to add into the data frame
			// here we assign it into the variable "dataframe"
			executeR( addTryEvalToScript( "datatable <- " + CREATE_DATA_TABLE_METHOD + "(" + r_row + ")"  ) );
			
			String header_row = createStringRColVec(headers);
			executeR( addTryEvalToScript( "setnames(datatable , " + header_row + ")" ) );
		}
		
		// now iterate through all the other rows
		while(it.hasNext()) {
			IHeadersDataRow row = it.next();
			String r_row = createRColVec(row.getValues(), types);
			// the r function is appendToDataTable.123456
			// it takes in the name of data table to append into and the list of elements to also add
			executeR( addTryEvalToScript( "datatable <- " + ADD_ROW_TO_DATA_TABLE_METHOD + "(datatable, " + r_row + ")"  ) );
		}
		
		// TODO: should i trim automatically?
		executeR( addTryEvalToScript( "datatable <- " + REMOVE_EMPTY_ROWS + "(datatable)"  ) );
	}
	
	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	private String createRColVec(Object[] row, IMetaData.DATA_TYPES[] dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			if(dataType[i] == IMetaData.DATA_TYPES.STRING) {
				str.append("\"").append(row[i]).append("\"");
			} else {
				str.append(row[i]);
			}
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	private String createStringRColVec(Object[] row) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			str.append("\"").append(row[i]).append("\"");
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	/**
	 * Calculate the number of rows in the data table
	 * @return
	 */
	protected int getNumRows() {
		REXP result = executeR( addTryEvalToScript( "nrow(datatable)"  ) );
		int numRows = 0;
		try {
			numRows = result.asInteger();
		} catch (REXPMismatchException e) {
			handleRException(result, e, "Error in calculating the number of rows for the datatable");
		}
		return numRows;
	}
	
	protected Iterator<Object[]> iterator(String[] headerNames) {
		return new RIterator(this, headerNames);
	}
	
	/**
	 * Determine if the datatable is empty
	 * @return
	 */
	public boolean isEmpty() {
		REXP result = executeR( addTryEvalToScript( "exists(datatable)" ) );
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
			handleRException(result, e, "Couldn't check if datatable exists");
		}
		
		return true;
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
