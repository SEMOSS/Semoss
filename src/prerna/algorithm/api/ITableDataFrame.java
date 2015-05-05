package prerna.algorithm.api;

import java.util.Map;
import java.util.Vector;

public interface ITableDataFrame {

	/**
	 * Generates an empty data-frame
	 */
	public void create();
	
	/**
	 * Joins the inputed data-frame to the current data-frame with a given confidence threshold for 
	 * the joining algorithm 
	 * @param tabel					The data-frame to join with the current data-frame
	 * @param confidenceThreshold	The confidence interval for the joining algorithm, should be in range [0,1]
	 * @param routine				The analytical routine to perform the joining
	 * @return						The resulting data-frame as a result of the join
	 */
	public ITableDataFrame join(ITableDataFrame tabel, double confidenceThreshold, IAnalytics routine);
	
	/**
	 * Joins the inputed data-frame to the current data-frame for the provided column names 
	 * @param tabel					The data-frame to join with the current data-frame
	 * @param colNameInTabel		The column name in the original data-frame to join
	 * @param routine				The column name in the inputed data-frame to join
	 * @return						The resulting data-frame as a result of the join
	 */
	public ITableDataFrame join(ITableDataFrame tabel, String colNameInTabel, String colNameInJoiningTable);
	
	/**
	 * Undo the most recent join on the data-table
	 * @return						The previous data-frame prior to performing the join
	 */
	public ITableDataFrame undoJoin();

	/**
	 * Append the inputed data-frame to the current data-frame
	 * @param tabel					The data-frame to append with the current data-frame
	 * @return						The resulting data-frame as a result of the append
	 */
	public ITableDataFrame append(ITableDataFrame tabel);
	
	/**
	 * Undo the most recent append to the data-frame
	 * @return						The previous data-frame prior to performing the append
	 */
	public ITableDataFrame undoAppend();

	/**
	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param routine				The IAnalytics routine to perform onto the data-frame
	 * @return						The resulting data-frame as a result of the routine
	 */
	public ITableDataFrame performAction(IAnalytics routine);
	
	/**
	 * Undo the most recent analytical routine performed on the data-frame
	 * @return						The previous data-frame prior to performing the analytical routine
	 */
	public ITableDataFrame undoAction();
	
	/**
	 * Generate the entropy for the column in the data-frame
	 * @param columHeader			The column header to calculate the entropy for
	 * @return						The entropy value for the column
	 */
	public Double getEntropy(String columHeader);

	/**
	 * Generate the entropy for all the columns in the data-frame
	 * @return						The entropy values for all the columns corresponding to the ordered values in the column headers
	 */
	public Double[] getEntropy();
	
	/**
	 * Generate the entropy density for the column in the data-frame
	 * @param columHeader			The column header to calculate the entropy density for
	 * @return						The entropy density value for the column
	 */
	public Double getEntropyDensity(String columHeader);

	/**
	 * Generate the entropy density for all the columns in the data-frame
	 * @return						The entropy density values for all the columns corresponding to the ordered values in the column headers
	 */
	public Double[] getEntropyDensity();

	/**
	 * Get the unique instance count for the column in the data-frame
	 * @param columHeader			The column header to get the unique instance count
	 * @return						The number of unique instances in the column
	 */
	public Integer getUniqueInstanceCount(String columHeader);

	/**
	 * Get the unique instance counts for all the columns in the data-frame
	 * @return						The unique instance counts for all columns corresponding to the ordered values in the column headers
	 */
	public Integer[] getUniqueInstanceCount();
	
	/**
	 * Get the maximum value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columHeader			The column header to get the maximum value
	 * @return						The maximum value in the column
	 */
	public Double getMax(String columHeader);

	/**
	 * Get the maximum value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The maximum value for all columns corresponding to the ordered values in the column headers
	 */
	public Double[] getMax();
	
	/**
	 * Get the minimum value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columHeader			The column header to get the minimum value
	 * @return						The minimum value in the column
	 */
	public Double getMin(String columHeader);

	/**
	 * Get the minimum value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The minimum value for all columns corresponding to the ordered values in the column headers
	 */
	public Double[] getMin();
	
	/**
	 * Get the average value for the column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columHeader			The column header to get the average value
	 * @return						The average value in the column
	 */
	public Double getAverage(String columHeader);

	/**
	 * Get the average value for all the columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The average value for all columns corresponding to the ordered values in the column headers
	 */
	public Double[] getAverage();
	
	/**
	 * Get the sum of the values for the inputed column in the data-frame
	 * Will return null if the column is non-numeric
	 * @param columHeader			The column header to get the sum of all the values
	 * @return						The sum of all the values in the column
	 */
	public Double getSum(String columHeader);

	/**
	 * Get the sum of the values for all the inputed columns in the data-frame
	 * Will return null in the column positions that are non-numeric
	 * @return						The sum of all the values for all columns corresponding to the ordered values in the column headers
	 */
	public Double[] getSum();
	
	/**
	 * Determine if a column is numeric or categorical
	 * @param columnHeader			The column header to determine if it is numeric or categorical
	 * @return						Boolean true if the column is numerical, false if it is categorical
	 */
	public boolean isNumeric(String columnHeader);
	
	/**
	 * Determine for all columns if the data is numeric or categorical
	 * @return						Boolean true if the column is numerical, false if it is categorical, for the ordered values in the column headers
	 */
	public boolean[] isNumeric();
	
	/**
	 * Get the column header names for the data-frame
	 * @return						The column header names for the data-frame
	 */
	public Vector<String> getColumnHeaders();
	
	/**
	 * Get the total number of columns in the data-frame
	 * @return						The count of the number of columns in the data-frame
	 */
	public int getNumCols();

	/**
	 * Get the total number of rows in the data-frame
	 * @return						The count of the number of rows in the data-frame
	 */
	public int getNumRows();
	
	/**
	 * Get the number of total values for the column in the data-frame
	 * @param columnHeader			The column header to get the total number of values
	 * @return						The count of the number of values for the column
	 */
	public int getColCount(String columnHeader);
	
	/**
	 * Get the number of total values for the row  
	 * @param rowIdx
	 * @return
	 */
	public int getRowCount(int rowIdx);
	
	/**
	 * Get the values for a specific row in the data-frame
	 * @param rowIdx				The row index for the data-frame
	 * @return						The values for the specific row index in the data-frame
	 */
	public Object[] getRow(int rowIdx);
	
	/**
	 * Get the values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The values for the specific column header in the data-frame
	 */
	public Object[] getColumn(String columnHeader);
	
	/**
	 * Get the unique column values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The unique values for the specific column header in the data-frame
	 */
	public Object[] getUniqueColumnValues(String columnHeader);
	
	/**
	 * Get the counts for each unique value in a specific column in the data-frame
	 * @param columnHeader			The column header to get the values and counts for
	 * @return						A mapping between the unique instance values and the count of the value
	 */
	public Map<String, Integer> getUniqueColumnValuesAndCount(String columnHeader);
	
	/**
	 * Get the counts for all the unique values in all the columns of the data-frame
	 * @return						A mapping between the column headers to a map between the unique instances of the column header to the count of the value
	 */
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount();
}
