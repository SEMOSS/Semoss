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
	void setSelectedOptions(Map<String, Object> selected);
	
	/**
	 * Get the list of parameter options used for the analytic routine
	 * @return					The list of parameters required for the analytic routine
	 */
	List<SEMOSSParam> getOptions();
	
	/**
	 * Get the default visualization type for the algorithm output
	 * @return
	 */
	String getDefaultViz();
	
	/**
	 * Get the meta-data for the results of the algorithm 
	 * @return
	 */
	Map<String, Object> getResultMetadata();
	
}
