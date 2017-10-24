package prerna.sablecc2.reactor.frame.r.util;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.om.Insight;

public interface IRJavaTranslator {
	
	String R_CONN = "R_CONN";
	String R_PORT = "R_PORT";
	String R_ENGINE = "R_ENGINE";
	String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";
	
	/**
	 * start r server
	 */
	void startR();
	
	/**
	 * Execute an R Script
	 * @param rScript
	 */
	Object executeR(String rScript);
	
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
	 * Retrieve a factor from an R Script
	 * @param rScript
	 */
	Object getFactor(String rScript);
	
	/**
	 * This method uses specific Rserve or JRI parseAndEval methods
	 * @throws IOException 
	 */
	Object parseAndEvalScript(String script) throws IOException;
	
	/**
	 * This method uses specific Rserve or JRI methods to get the breaks for a histogram as double[]
	 * @param script
	 */
	double[] getHistogramBreaks(String script);
	
	/**
	 * This method uses specific Rserve or JRI methods to get the counts for a histogram as int[]
	 * @param script
	 */
	int[] getHistogramCounts(String script);
	
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
	
	void endR();
	
}
