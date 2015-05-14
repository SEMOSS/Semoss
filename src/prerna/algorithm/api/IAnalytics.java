package prerna.algorithm.api;

import java.util.Map;

public interface IAnalytics {

	/**
	 * Set the options for the analytic routines
	 * @param options			A mappings of the option type and their values
	 */
	void setOptions(Map options);
	
	/**
	 * Get the options used for the analytic routine
	 * @return					A mappings of the option type and their values
	 */
	Map getOptions();
	
	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				The data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
	ITableDataFrame runAlgorithm(ITableDataFrame data);
	
}
