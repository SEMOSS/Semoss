package prerna.sablecc2.reactor.frame.r.util;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;

public interface IRJavaTranslator {
	
	String R_CONN = "R_CONN";
	String R_PORT = "R_PORT";
	String R_ENGINE = "R_ENGINE";
	String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";
	
	/**
	 * Initialize the environment 
	 */
	void initREnv();

	/**
	 * Initialize the environment 
	 */
	void initREnv(String env);

	/**
	 * start r server
	 */
	void startR();
	
	/**
	 * Execute an R Script
	 * YOU SHOULD ONLY BE USING THIS WHEN YOU NEED THE RETURN
	 * OTHERWISE, USE executeEmptyR
	 * @param rScript
	 */
	Object executeR(String rScript);

	/**
	 * Execute an R Script without a return
	 * @param rScript
	 */
	void executeEmptyR(String rScript);

	/**
	 * Cancel the execution of the currently running R script. Different from
	 * stopRProcess in that the R service still runs. Similar to stop in R Studio.
	 * @return cancelled
	 */
	boolean cancelExecution();
	
	/**
	 * Run a combination of r scripts
	 * @param rScript
	 */
	void runR(String rScript);
	
	/**
	 * Run a combination of r scripts
	 * @param rScript
	 */
	String runRAndReturnOutput(String rScript);

	/**
	 * Run a combination of r scripts
	 * @param rScript
	 * @param appMap - custom var set for folder names
	 */
	String runRAndReturnOutput(String rScript, Map appMap);

	
	/**
	 * Get a string from an r script
	 * @param script
	 * @return
	 */
	String getString(String script);
	
	/**
	 * Get a string array from an r script
	 * @param script
	 * @return
	 */
	String[] getStringArray(String script);
	
	/**
	 * Get a int from an r script
	 * @param script
	 * @return
	 */
	int getInt(String script);
	
	/**
	 * Retrieve an int Array from an R Script
	 * @param rScript
	 */
	int[] getIntArray(String rScript);
	
	
	/**
	 * Retrieve a double from an R Script
	 * @param rScript
	 */
	double getDouble(String rScript);
	
	/**
	 * Retrieve a double Array from an R Script
	 * @param rScript
	 */
	double[] getDoubleArray(String rScript);
	
	/**
	 * Retrieve a double matrix from an R script
	 * @param rScript
	 * @return
	 */
	double[][] getDoubleMatrix(String rScript);
	
	/**
	 * Retrieve a boolean
	 * @param rScript
	 * @return
	 */
	boolean getBoolean(String rScript);
	
	/**
	 * Retrieve a factor from an R Script
	 * @param rScript
	 */
	//TODO: why is this an object
	//TODO: why is this an object
	//TODO: why is this an object
	//TODO: why is this an object
	//TODO: why is this an object
	Object getFactor(String rScript);
	
	/**
	 * Set the insight
	 * @param insight
	 */
	void setInsight(Insight insight);
	
	/**
	 * Set the logger
	 * @param logger
	 */
	void setLogger(Logger logger);
	
	/**
	 * Set the connection object for Rserve
	 * @param connection
	 */
	void setConnection(RConnection connection);
	
	/**
	 * Set the port for Rserve
	 * @param port
	 */
	void setPort(String port);
	
	/**
	 * End the R
	 */
	void endR();
	
	/**
	 * Stop R process
	 */
	void stopRProcess();
	
	public String[] getColumnTypes(String frameName);

	public boolean isEmpty(String frameName);
	
	public boolean varExists(String varname);
	
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert) ;
	
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert, SemossDataType currentType);
	
	public String getColumnType(String frameName, String column);
	
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat);
	
	public int getNumRows(String frameName);
	
	public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols);
	
	public void checkPackages(String[] packages);
	
    public boolean checkPackages(String[] packages, Logger logger);
    
}
