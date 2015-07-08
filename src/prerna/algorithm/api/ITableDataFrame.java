package prerna.algorithm.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.ISelectStatement;

public interface ITableDataFrame {

	/**
	 * Adds a row to the data-frame
	 * @param statement				The query result to add to the data-frame
	 */
	void addRow(ISelectStatement statement);
	
	/**
	 * Adds a row to the data-frame
	 * @param rowCleanData			The map between the column and the clean value for the row being added
	 * @param rowRawData			The map between the column and the raw value for the row being added
	 */
	void addRow(Map<String, Object> rowCleanData, Map<String, Object> rowRawData);
	
	/**
	 * Gets the most similar columns given threshold and routine between this and the passed in table.
	 * This can be used before calling join to better understand the two tables
	 * @param table					The data-frame to join with the current data-frame
	 * @param confidenceThreshold	The confidence interval for the joining algorithm, should be in range [0,1]
	 * @param routine				The analytical routine to perform the joining
	 * @return						The column headers that are most similar from the two tables. First value is from this table, second value is from passed in table
	 */
	List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine);
	
	/**
	 * Joins the inputed data-frame to the current data-frame for the provided column names 
	 * @param table					The data-frame to join with the current data-frame
	 * @param colNameInTable		The column name in the original data-frame to join
	 * @param colNameInJoiningTable	The column name in the inputed data-frame to join
	 * @param confidenceThreshold	The confidence interval for the joining algorithm, should be in range [0,1]
	 * @param routine				The analytical routine to perform the joining
	 */
	void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IAnalyticRoutine routine);
	
	/**
	 * Undo the most recent join on the data-table
	 * @return						The previous data-frame prior to performing the join
	 */
	void undoJoin();

	/**
	 * Append the inputed data-frame to the current data-frame
	 * @param table					The data-frame to append with the current data-frame
	 */
	void append(ITableDataFrame table);
	
	/**
	 * Undo the most recent append to the data-frame
	 */
	void undoAppend();

	/**
	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param routine				The IAnalytics routine to perform onto the data-frame
	 */
	void performAction(IAnalyticRoutine routine);
	
	/**
	 * Undo the most recent analytical routine performed on the data-frame
	 */
	void undoAction();
	
	/**
	 * Generate the entropy for the column in the data-frame
	 * @param columnHeader			The column header to calculate the entropy for
	 * @return						The entropy value for the column
	 */
	Double getEntropy(String columnHeader);

	/**
	 * Generate the entropy for all the columns in the data-frame
	 * @return						The entropy values for all the columns corresponding to the ordered values in the column headers
	 */
	Double[] getEntropy();
	
	/**
	 * Generate the entropy density for the column in the data-frame
	 * @param columnHeader			The column header to calculate the entropy density for
	 * @return						The entropy density value for the column
	 */
	Double getEntropyDensity(String columnHeader);

	/**
	 * Generate the entropy density for all the columns in the data-frame
	 * @return						The entropy density values for all the columns corresponding to the ordered values in the column headers
	 */
	Double[] getEntropyDensity();

	/**
	 * Get the unique instance count for the column in the data-frame
	 * @param columnHeader			The column header to get the unique instance count
	 * @return						The number of unique instances in the column
	 */
	Integer getUniqueInstanceCount(String columnHeader);

	/**
	 * Get the unique instance counts for all the columns in the data-frame
	 * @return						The unique instance counts for all columns corresponding to the ordered values in the column headers
	 */
	Integer[] getUniqueInstanceCount();
	
	/**
	 * Get the maximum value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columnHeader			The column header to get the maximum value
	 * @return						The maximum value in the column
	 */
	Double getMax(String columnHeader);

	/**
	 * Get the maximum value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The maximum value for all columns corresponding to the ordered values in the column headers
	 */
	Double[] getMax();
	
	/**
	 * Get the minimum value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columnHeader			The column header to get the minimum value
	 * @return						The minimum value in the column
	 */
	Double getMin(String columnHeader);

	/**
	 * Get the minimum value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The minimum value for all columns corresponding to the ordered values in the column headers
	 */
	Double[] getMin();
	
	/**
	 * Get the average value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columnHeader			The column header to get the average value
	 * @return						The average value in the column
	 */
	Double getAverage(String columnHeader);

	/**
	 * Get the average value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The average value for all columns corresponding to the ordered values in the column headers
	 */
	Double[] getAverage();
	
	/**
	 * Get the sum of the values for the inputed column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columnHeader			The column header to get the sum of all the values
	 * @return						The sum of all the values in the column
	 */
	Double getSum(String columnHeader);

	/**
	 * Get the sum of the values for all the inputed columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The sum of all the values for all columns corresponding to the ordered values in the column headers
	 */
	Double[] getSum();
	
	/**
	 * Get the standard deviation of the values for the inputed column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columnHeader			The column header to get the sum of all the values
	 * @return						The standard deviation of all the values in the column
	 */
	Double getStandardDeviation(String columnHeader);

	/**
	 * Get the standard deviation of the values for all the inputed columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The standard deviation of all the values for all columns corresponding to the ordered values in the column headers
	 */
	Double[] getStandardDeviation();
	
	/**
	 * Determine if a column is numeric or categorical
	 * @param columnHeader			The column header to determine if it is numeric or categorical
	 * @return						Boolean true if the column is numerical, false if it is categorical
	 */
	boolean isNumeric(String columnHeader);
	
	/**
	 * Determine for all columns if the data is numeric or categorical
	 * @return						Boolean true if the column is numerical, false if it is categorical, for the ordered values in the column headers
	 */
	boolean[] isNumeric();
	
	/**
	 * Get the column header names for the data-frame
	 * @return						The column header names for the data-frame
	 */
	String[] getColumnHeaders();
	
	/**
	 * Get the total number of columns in the data-frame
	 * @return						The count of the number of columns in the data-frame
	 */
	int getNumCols();

	/**
	 * Get the total number of rows in the data-frame
	 * @return						The count of the number of rows in the data-frame
	 */
	int getNumRows();
	
	/**
	 * Get the number of total values for the column in the data-frame
	 * @param columnHeader			The column header to get the total number of values
	 * @return						The count of the number of values for the column
	 */
	int getColCount(int rowIdx);
	
	/**
	 * Get the number of total values for the row  
	 * @param rowIdx
	 * @return
	 */
	int getRowCount(String columnHeader);
	
	/**
	 * Iterator to go through all the rows in the data-frame
	 * The iterator will return an Object[] corresponding to the data in a row of the data-frame
	 * @return						The iterator to go through all the rows
	 */
	Iterator<Object[]> iterator();
	
	/**
	 * Iterator to go through all the rows in the data-frame and return all the values in unique-valued groups based on a specific column
	 * The iterator will return a List<Object[]> corresponding to the data in a row of the data-frame
	 * @return						The iterator to go through all the rows
	 */
	Iterator<List<Object[]>> uniqueIterator(String columnHeader);
	
	/**
	 * Get the values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The values for the specific column header in the data-frame
	 */
	Object[] getColumn(String columnHeader);
	
	/**
	 * Get the values for a specific column in the data-frame
	 * If column is non-numeric, returns null
	 * otherwise returns an array of Doubles, with Null as the placeholder for EMPTY values
	 * @param columnHeader			The column header to get the values for
	 * @return						The values for the specific column header in the data-frame
	 */
	Double[] getColumnAsNumeric(String columnHeader);
	/**
	 * Get the raw values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The raw values for the specific column header in the data-frame
	 */
	Object[] getRawColumn(String columnHeader);
		
	/**
	 * Get the unique column values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The unique values for the specific column header in the data-frame
	 */
	Object[] getUniqueValues(String columnHeader);
	
	/**
	 * Get the counts for each unique value in a specific column in the data-frame
	 * @param columnHeader			The column header to get the values and counts for
	 * @return						A mapping between the unique instance values and the count of the value
	 */
	Map<String, Integer> getUniqueValuesAndCount(String columnHeader);
	
	/**
	 * Get the counts for all the unique values in all the columns of the data-frame
	 * @return						A mapping between the column headers to a map between the unique instances of the column header to the count of the value
	 */
	Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount();
	
	/**
	 * Refreshes data-frame with what is in database
	 */
	void refresh();
	
	/**
	 * Filter table based on passed in values
	 * @param columnHeader			The column header to apply the filter on
	 * @param filterValues			The specific values of the column header for the filtering
	 */
	void filter(String columnHeader, List<Object> filterValues);

	/**
	 * Unfilter all values for the passed in column header
	 * @param columnHeader			The column header to remove the filter on
	 * @return						The data-frame with the filtering applied
	 */
	void unfilter(String columnHeader);

	/**
	 * Removes a column from the data-frame
	 * @param columnHeader			The column header to remove from the data-frame
	 */
	void removeColumn(String columnHeader);
	
	/**
	 * Removes a row from the data-frame
	 * @param rowIdx				The row to remove from the data-frame
	 */
	void removeRow(int rowIdx);
	
	/**
	 * Splits the data-frame into two parts based on the inputed column header
	 * Defaulted that the column header passed in is only included in the left partition of the split 
	 * @param columnHeader			The column header to split the data-frame
	 * @return						An array of data-frames containing the two partitions of the split
	 */
	ITableDataFrame[] splitTableByColumn(String columnHeader);
	
	/**
	 * Splits the data-frame into two parts based on the inputed row index
	 * Defaulted that the row index passed in is only included in the left partition of the split
	 * @param rowIdx				The row index to split the data-frame
	 * @return						An array of data-frames containing the two partitions of the split
	 */
	ITableDataFrame[] splitTableByRow(int rowIdx);
	
	/**
	 * Get all the data contained in the data-frame
	 * @return						An ArrayList of Object arrays containing all the data
	 */
	List<Object[]> getData();
	
	/**
	 * Get all the raw data contained in the data-frame
	 * @return						An ArrayList of Object arrays containing all the raw data
	 */
	List<Object[]> getRawData();
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	List<Object[]> getData(String columnHeader, Object value);
}
