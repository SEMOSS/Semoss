package prerna.algorithm.api;

import java.util.List;
import java.util.Map;

import prerna.om.SEMOSSParam;

public interface IAnalyticRoutine {

	/**
	 * Get the name of the algorithm
	 * @return
	 */
	String getName();
	
	/**
	 * Get the description of the algorithm output
	 * @return
	 */
	String	getResultDescription();
	
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
	 * Get the list of parameter options used for the analytic routine
	 * @return					The list of parameters required for the analytic routine
	 */
	List<SEMOSSParam> getAllAlgorithmOptions();
	
	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				An array of data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
	ITableDataFrame runAlgorithm(ITableDataFrame... data);
	
	/**
	 * Get the default visualization type for the algorithm output
	 * @return
	 */
	String getDefaultViz();
	
	/**
	 * Get the list of the columns that have been altered as a result of the algorithm
	 * This will return null when no columns have been changed
	 * @return
	 */
	List<String> getChangedColumns();
	
	/**
	 * Get the meta-data for the results of the algorithm 
	 * @return
	 */
	Map<String, Object> getResultMetadata();
	
}
