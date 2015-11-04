package prerna.algorithm.api;

import java.util.List;

public interface IAnalyticTransformationRoutine extends IAnalyticRoutine{

	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				An array of data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
	ITableDataFrame runAlgorithm(ITableDataFrame... data);
	
	/**
	 * Get the list of the columns that have been altered as a result of the algorithm
	 * This will return null when no columns have been changed
	 * @return
	 */
	List<String> getChangedColumns();
	
}
