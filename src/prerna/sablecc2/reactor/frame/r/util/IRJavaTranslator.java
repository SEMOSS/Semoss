package prerna.sablecc2.reactor.frame.r.util;

import org.apache.log4j.Logger;



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
	 * Retrieve an int Array from an R Script
	 * @param rScript
	 */
	int[] getIntArray(String rScript);
	
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
	 * Get a string array from an r script
	 * @param script
	 * @return
	 */
	String[] getStringArray(String script);
	
	/**
	 * Get a string from an r script
	 * @param script
	 * @return
	 */
	String getString(String script);
	
}
