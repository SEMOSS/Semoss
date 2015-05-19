package prerna.algorithm.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.SEMOSSParam;

public interface IAnalyticRoutine {

	Map options = new HashMap();
	
	/**
	 * Set the options for the analytic routines
	 * @param options			A mappings of the option type and their values
	 */
	void setOptions(Map options);
	
	/**
	 * Get the options used for the analytic routine
	 * @return					A mappings of the option type and their values
	 */
	List<SEMOSSParam> getOptions();
	
	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				An array of data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
	ITableDataFrame runAlgorithm(ITableDataFrame... data);
	
	String getName();
	String getDefaultViz();
	List<String> getChangedColumns();
	Map<String, Object> getResultMetadata();
	String	getResultDescription();
	
}
