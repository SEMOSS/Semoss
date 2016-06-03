package prerna.algorithm.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public interface ITableDataFrame extends IDataMaker {
	
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
	 * Adds a row to the data-frame
	 * @param rowCleanData			The array of clean values where indices match the columns in the data-frame
	 * @param rowRawData			The array of raw values where indices match the columns in the data-frame
	 */
	void addRow(Object[] rowCleanData, Object[] rowRawData);

	/**
	 * Adds a row to the data-frame
	 * @param rowCleanData			The array of clean values where indices match the columns in the data-frame
	 * @param rowRawData			The array of raw values where indices match the columns in the data-frame
	 * @param headers				The headers corresponding to the new row to add
	 */
	void addRow(Object[] cleanCells, Object[] rawCells, String[] headers);

	/**
	 * 
	 * @param headers
	 * @param values
	 * @param rawValues
	 * @param cardinality
	 * @param logicalToValMap
	 */
	void addRelationship(String[] headers, Object[] values, Object[] rawValues, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap);

	/**
	 * Joins the inputed data-frame to the current data-frame for the provided column names 
	 * @param table					The data-frame to join with the current data-frame
	 * @param colNameInTable		The column name in the original data-frame to join
	 * @param colNameInJoiningTable	The column name in the inputed data-frame to join
	 * @param confidenceThreshold	The confidence interval for the joining algorithm, should be in range [0,1]
	 * @param routine				The analytical routine to perform the joining
	 */
	void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine);
	
	/**
	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param routine				The IAnalytics routine to perform onto the data-frame
	 */
	void performAnalyticTransformation(IAnalyticTransformationRoutine routine) throws RuntimeException;
	
	/**
	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param routine				The IAnalytics routine to perform onto the data-frame
	 */
	void performAnalyticAction(IAnalyticActionRoutine routine) throws RuntimeException;
	
	/**
	 * Generate the entropy density for the column in the data-frame
	 * @param columnHeader			The column header to calculate the entropy density for
	 * @return						The entropy density value for the column
	 */
	Double getEntropyDensity(String columnHeader);

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
	 * Get the total number of rows in the data-frame
	 * @return						The count of the number of rows in the data-frame
	 */
	int getNumRows();
	
	/**
	 * Iterator to go through all the rows in the data-frame
	 * The iterator will return an Object[] corresponding to the data in a row of the data-frame
	 * @return						The iterator to go through all the rows
	 */
	Iterator<Object[]> iterator(boolean getRawData);
	
	/**
	 * Iterator to go through all the rows in the data-frame
	 * The iterator will return an Object[] corresponding to the data in a row of the data-frame
	 * @return						The iterator to go through all the rows
	 */
	Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options);
	
	/**
	 * Iterator to go through all the rows in the data-frame and return all the values in unique-valued groups based on a specific column
	 * The iterator will return a List<Object[]> corresponding to the data in a row of the data-frame
	 * @return						The iterator to go through all the rows
	 */
	Iterator<List<Object[]>> uniqueIterator(String columnHeader, boolean getRawData);
	
	/**
	 * 	 * Returns the iterator that will iterate through a numeric column
	 * the iterator will return the each unique value in the column as a function -> x' = (x - min(columnHeader))/(max(columnHeader) - min(columnHeader))	 * @param columnHeader
	 * @return
	 */
	Iterator<Object[]> scaledIterator(boolean getRawData);
	
	/**
	 * 	 * Returns the iterator that will iterate through a numeric column
	 * the iterator will return the each unique value in the column as a function -> x' = (x - min(columnHeader))/(max(columnHeader) - min(columnHeader))	 * @param columnHeader
	 * @return
	 */
	Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData, Map<String, Object> options);
	
	/**
	 * Returns the iterator that iterates through unique values of a column
	 * @param columnHeader 		Name of column to iterate through
	 * @param getRawData		get the raw data value if true, value otherwise
	 * @param iterateAll		iterate through filtered and unfiltered values if true, just unfiltered values otherwise
	 * @return
	 */
	Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll);
	
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
		
//	/**
//	 * Get the unique column values for a specific column in the data-frame
//	 * @param columnHeader			The column header to get the values for
//	 * @return						The unique values for the specific column header in the data-frame
//	 */
//	Object[] getUniqueValues(String columnHeader);
	
	/**
	 * Get the unique raw column values for a specific column in the data-frame
	 * @param columnHeader			The column header to get the values for
	 * @return						The unique raw values for the specific column header in the data-frame
	 */
	Object[] getUniqueRawValues(String columnHeader);
	
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

//	void unfilter(String columnHeader, List<Object> unfilterValues);
	/**
	 * Unfilter all columns for the data frame
	 */
	void unfilter();
	
	/**
	 * Removes a column from the data-frame
	 * @param columnHeader			The column header to remove from the data-frame
	 */
	void removeColumn(String columnHeader);
	
	/**
	 * Get all the data contained in the data-frame
	 * @return						An ArrayList of Object arrays containing all the data
	 */
	List<Object[]> getData();
	
	/**
	 * Get all the data contained in the data-frame including filteredData
	 * @return					An ArrayList of Object arrays containing all the data
	 */
	List<Object[]> getAllData();
	
	/**
	 * Get all the data with numeric columns scaled
	 * @return
	 */
	List<Object[]> getScaledData();
	
	/**
	 * Get all the data with numeric columns scaled with exceptions
	 * @param exceptionColumns
	 * @return
	 */
	List<Object[]> getScaledData(List<String> exceptionColumns);
	/**
	 * Get all the raw data contained in the data-frame
	 * @return						An ArrayList of Object arrays containing all the raw data
	 */
	List<Object[]> getRawData();
	
//	/**
//	 * 
//	 * @param value
//	 * @return
//	 */
//	List<Object[]> getData(String columnHeader, Object value);
	
	/**
	 * Returns if the ITable is empty
	 * @return
	 */
	boolean isEmpty();
	
	/**
	 * Bins a numeric column and adds it to the tree
	 * @param column
	 */
	void binNumericColumn(String column);
	
	/**
	 * Bins numeric columns and adds it to the tree
	 * @param columns
	 */
	void binNumericalColumns(String[] columns);
	
	/**
	 * Bins all numeric columns
	 * @param columns
	 */
	void binAllNumericColumns();
	
	/**
	 * 
	 * @param columnHeaders
	 */
	public void setColumnsToSkip(List<String> columnHeaders);
	
	/**
	 * 
	 * @param 
	 * @return the filter model associated with this table used for excel style filtering on the table
	 */
	Object[] getFilterModel();
	
	/**
	 * Serialize the dataframe
	 * @param fileName
	 */
	void save(String fileName);
	
	/**
	 * Deserialize the dataframe
	 * @param fileName
	 * @return
	 */
	ITableDataFrame open(String fileName);

	/**
	 * 
	 * @param primKeyEdgeHash
	 * @param dataTypeMap
	 */
	void mergeEdgeHash(Map<String, Set<String>> primKeyEdgeHash, Map<String, String> dataTypeMap);

	/**
	 * 
	 * @param edgeHash
	 * @param engine
	 * @param joinCols
	 * @return
	 */
	Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine, Vector<Map<String, String>> joinCols);

	/**
	 * 
	 * @param outType
	 * @param inType
	 * @param dataTypeMap
	 */
	void connectTypes(String outType, String inType, Map<String, String> dataTypeMap);

	/**
	 * 
	 * @param joinCols
	 * @param newCol
	 * @param dataTypeMap
	 */
	void connectTypes(String[] joinCols, String newCol, Map<String, String> dataTypeMap);

	/**
	 * 
	 * @param cleanRow
	 * @param rawRow
	 */
	void addRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow);

	/**
	 * 
	 * @param cleanRow
	 * @param rawRow
	 */
	void removeRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow);
	
	/**
	 * 
	 * @return
	 */
	Map<String, Set<String>> getEdgeHash();

	/**
	 * 
	 * @param sub
	 * @return
	 */
	Set<String> getEnginesForUniqueName(String sub);

	/**
	 * 
	 * @return
	 */
	Map<String, String> getProperties();

	/**
	 * 
	 * @param string
	 * @param engineName
	 * @return
	 */
	String getPhysicalUriForNode(String string, String engineName);

	/**
	 * 
	 * @return
	 */
	List<Map<String, Object>> getTableHeaderObjects();

	/**
	 * 
	 * @param rowCleanData
	 * @param rowRawData
	 * @param edgeHash
	 * @param logicalToValMap
	 */
	void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToValMap);
	
	/**
	 * 
	 * @return
	 */
	Map<String, Object[]> getFilterTransformationValues();

	/**
	 * 
	 * @param columnHeader
	 * @param filterValues
	 * @param comparator
	 */
	void filter(String columnHeader, List<Object> filterValues, String comparator);
	
	/**
	 * 
	 * @param uniqueName
	 * @param isDerived
	 */
	void setDerivedColumn(String uniqueName, boolean isDerived);
	
	/**
	 * 
	 * @param uniqueName
	 * @param calculationName
	 */
	void setDerviedCalculation(String uniqueName, String calculationName);
	
	/**
	 * 
	 * @param uniqueName
	 * @param otherUniqueNames
	 */
	void setDerivedUsing(String uniqueName, String... otherUniqueNames);
	
	/**
	 * 
	 * @param uniqueName
	 * @return
	 */
	DATA_TYPES getDataType(String uniqueName);
}
