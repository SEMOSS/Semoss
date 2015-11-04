package prerna.algorithm.api;

public interface IAnalyticActionRoutine extends IAnalyticRoutine {

	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				An array of data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
	void runAlgorithm(ITableDataFrame... data);
	
	/**
	 * Gets the output for the specific analytic action
	 * @return					The object containing the actions return
	 */
	Object getAlgorithmOutput();
	
}
