package prerna.algorithm.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;

import org.apache.logging.log4j.Logger;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.shared.CachedIterator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

/**
 * Interface that defines the core functionality for tabular data frame operations.
 * This interface extends {@link IDataMaker} and provides comprehensive methods for
 * data manipulation, querying, filtering, and metadata management in a tabular format.
 * 
 * <p>
 * The ITableDataFrame represents a structured data container that supports operations
 * such as adding rows, determining column types, applying filters, querying data,
 * and managing persistence. It serves as the primary abstraction for working with
 * tabular data in the system.
 * </p>
 * 
 * <p>
 * Implementations of this interface handle different underlying data storage mechanisms
 * while providing a consistent API for data frame operations. The interface supports
 * both numerical and categorical data types, complex filtering operations, and
 * various export formats.
 * </p>
 * 
 * @see {@link IDataMaker} for base data making functionality
 * @see {@link DataFrameTypeEnum} for different data frame implementations
 * @see {@link GenRowFilters} for filtering capabilities
 */
public interface ITableDataFrame extends IDataMaker {
	
	/**
	 * Adds a row to the data frame with the specified values and headers.
	 *
	 * @param cleanCells The array of clean values where indices match the columns in the data frame.
	 * @param headers The headers corresponding to the new row to add.
	 */
	void addRow(Object[] cleanCells, String[] headers);
	
	/**
	 * Determines if a column contains numeric or categorical data.
	 *
	 * @param columnHeader The column header to determine if it is numeric or categorical.
	 * @return True if the column is numerical, false if it is categorical.
	 */
	boolean isNumeric(String columnHeader);
	
	/**
	 * Determines for all columns if the data is numeric or categorical.
	 *
	 * @return Array of boolean values where true indicates the column is numerical and false indicates categorical, 
	 *         ordered according to the column headers.
	 */
	boolean[] isNumeric();
	
	/**
	 * Gets the clean column frame headers.
	 *
	 * @return Array of column header names for the data frame.
	 */
	String[] getColumnHeaders();
	
	/**
	 * Gets the query structure names for the data frame.
	 *
	 * @return Array of column header names used in query structures for the data frame.
	 */
	String[] getQsHeaders();
	
	/**
	 * Returns an iterator that will iterate through a numeric column with scaled unique values.
	 * The iterator returns each unique value in the column as a scaled function where 
	 * x' = (x - min(columnHeader))/(max(columnHeader) - min(columnHeader)).
	 *
	 * @param uniqueHeaderName The column header for unique values to scale.
	 * @param attributeUniqueHeaderName List of attribute headers for unique values.
	 * @return Iterator over lists of object arrays containing scaled unique values.
	 */
	Iterator<List<Object[]>> scaledUniqueIterator(String uniqueHeaderName, List<String> attributeUniqueHeaderName);
	
	/**
	 * Gets the values for a specific column in the data frame.
	 *
	 * @param columnHeader The column header to get the values for.
	 * @return Array of values for the specific column header in the data frame.
	 */
	Object[] getColumn(String columnHeader);
	
	/**
	 * Gets the values for a specific column in the data frame as numeric values.
	 * If the column is non-numeric, returns null. Otherwise returns an array of 
	 * Double values, with null as the placeholder for empty values.
	 *
	 * @param columnHeader The column header to get the values for.
	 * @return Array of Double values for the specific column header, or null if the column is non-numeric.
	 */
	Double[] getColumnAsNumeric(String columnHeader);
	
	/**
	 * Adds a filter to the frame and persists it.
	 *
	 * @param filter The {@link GenRowFilters} to add to the frame.
	 */
	void addFilter(GenRowFilters filter);

	/**
	 * Adds a query filter to the frame.
	 *
	 * @param filter The {@link IQueryFilter} to add to the frame.
	 */
	void addFilter(IQueryFilter filter);
	
	/**
	 * Sets a filter on the frame, overriding any existing filter for the same column.
	 * This method will replace existing filters rather than adding to them.
	 *
	 * @param filter The {@link GenRowFilters} to set on the frame.
	 */
	void setFilter(GenRowFilters filter);

	/**
	 * Gets the current filters applied to the frame.
	 *
	 * @return The {@link GenRowFilters} object containing all active filters.
	 */
	GenRowFilters getFrameFilters();
	
	/**
	 * Sets a completely new frame filters object, replacing any existing filters.
	 *
	 * @param filter The new {@link GenRowFilters} object to set for the frame.
	 */
	void setFrameFilters(GenRowFilters filter);
	
	/**
	 * Removes all filters for the specified column header.
	 *
	 * @param columnHeader The column header to remove the filter from.
	 * @return True if the unfiltering was successful, false otherwise.
	 */
	boolean unfilter(String columnHeader);

	/**
	 * Removes all filters from all columns in the data frame.
	 *
	 * @return True if the unfiltering was successful, false otherwise.
	 */
	boolean unfilter();
	
	/**
	 * Removes a column from the data frame.
	 *
	 * @param columnHeader The column header to remove from the data frame.
	 */
	void removeColumn(String columnHeader);
	
	/**
	 * Determines if the data frame is empty (contains no data).
	 *
	 * @return True if the data frame is empty, false otherwise.
	 */
	boolean isEmpty();
	
	/**
	 * Serializes the data frame to persistent storage.
	 *
	 * @param folderDir The directory path where the data frame should be saved.
	 * @param cipher The encryption cipher to use for securing the saved data.
	 * @return A {@link CachePropFileFrameObject} representing the saved data frame.
	 * @throws IOException If an error occurs during the serialization process.
	 */
	CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException;
	
	/**
	 * Deserializes and opens a data frame from persistent storage.
	 *
	 * @param cf The {@link CachePropFileFrameObject} containing the saved data frame information.
	 * @param cipher The decryption cipher to use for accessing the saved data.
	 * @throws IOException If an error occurs during the deserialization process.
	 */
	void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException;

	/**
	 * Gets the number of rows for the specified table in the frame.
	 *
	 * @param tableName The name of the table to count rows for.
	 * @return The number of rows in the specified table.
	 */
	long size(String tableName);
	
	/**
	 * Executes a query against the data frame using a string query.
	 *
	 * @param query The query string to execute against the data frame.
	 * @return An {@link IRawSelectWrapper} containing the query results.
	 * @throws Exception If an error occurs during query execution.
	 */
	IRawSelectWrapper query(String query) throws Exception;
	
	/**
	 * Executes a query against the data frame using a structured query object.
	 *
	 * @param qs The {@link SelectQueryStruct} containing the structured query to execute.
	 * @return An {@link IRawSelectWrapper} containing the query results.
	 * @throws Exception If an error occurs during query execution.
	 */
	IRawSelectWrapper query(SelectQueryStruct qs) throws Exception;
	
	/**
	 * Gets the query interpreter associated with this data frame.
	 *
	 * @return The {@link IQueryInterpreter} used to process queries for this data frame.
	 */
	IQueryInterpreter getQueryInterpreter();
	
	/**
	 * Gets the current name of the frame.
	 *
	 * @return The current name of the data frame.
	 */
	String getName();
	
	/**
	 * Sets the name of the frame.
	 *
	 * @param name The new name to assign to the data frame.
	 */
	void setName(String name);
	
	/**
	 * Gets the original name of the frame.
	 *
	 * @return The original name of the data frame before any renaming operations.
	 */
	String getOriginalName();
	
	/**
	 * Sets the original name of the frame.
	 *
	 * @param name The original name to assign to the data frame.
	 */
	void setOriginalName(String name);
	
	/**
	 * Gets the metadata associated with this data frame.
	 *
	 * @return The {@link OwlTemporalEngineMeta} metadata object for this data frame.
	 */
	OwlTemporalEngineMeta getMetaData();

	/**
	 * Sets the metadata for this data frame.
	 *
	 * @param metaData The {@link OwlTemporalEngineMeta} metadata object to associate with this data frame.
	 */
	void setMetaData(OwlTemporalEngineMeta metaData);

	/**
	 * Synchronizes the headers of the data frame to ensure consistency.
	 */
	void syncHeaders();
	
	/**
	 * Gets a map of frame header objects for the specified header types.
	 *
	 * @param headerTypes Variable number of header type strings to retrieve.
	 * @return A map where keys are header names and values are header objects.
	 */
	Map<String, Object> getFrameHeadersObject(String... headerTypes);
	
	/**
	 * Sets the logger for this data frame.
	 *
	 * @param logger The {@link Logger} instance to use for logging operations.
	 */
	void setLogger(Logger logger);
	
	/**
	 * Closes and deletes the frame, releasing any associated resources.
	 */
	void close();
	
	/**
	 * Determines if the frame has been closed.
	 *
	 * @return True if the frame has been closed, false otherwise.
	 */
	boolean isClosed();
	
	//////////////////////////////////////////////////
	
	// Info that is cached on the frame
	
	/**
	 * Determines if the specified column contains unique values within the frame.
	 * Note: This assumes the column is part of a table. Even if using a native frame
	 * with joins, the result set where this column is returned is still part of a single table.
	 *
	 * @param columnName The name of the column to check for uniqueness.
	 * @return True if the column contains unique values, false otherwise, or null if unknown.
	 */
	Boolean isUniqueColumn(String columnName);
	
	/**
	 * Clears any cached information on the frame since it is no longer valid.
	 * This method should be called when the frame data has been modified in ways
	 * that invalidate previously cached metrics.
	 */
	void clearCachedMetrics();
	
	/**
	 * Caches a query iterator on the frame for performance optimization.
	 *
	 * @param it The {@link CachedIterator} to cache for future query operations.
	 */
	void cacheQuery(CachedIterator it);
	
	/**
	 * Clears the cached queries due to modifications to the data.
	 * This method should be called when the underlying data has changed
	 * and cached query results are no longer valid.
	 */
	void clearQueryCache();
	
	/**
	 * Returns the type of this data frame.
	 *
	 * @return The {@link DataFrameTypeEnum} representing the implementation type of this data frame.
	 */
	DataFrameTypeEnum getFrameType();
	
	/**
	 * Executes an SQL query and returns the result as a generic object.
	 *
	 * @param sql The SQL query string to execute.
	 * @return The query result as an object.
	 */
	Object querySQL(String sql);

	/**
	 * Executes a query and returns the result in CSV format.
	 *
	 * @param sql The query string to execute.
	 * @return The query result formatted as CSV.
	 */
	Object queryCSV(String sql);

	/**
	 * Executes a query and returns the result in JSON format.
	 *
	 * @param sql The query string to execute.
	 * @return The query result formatted as JSON.
	 */
	Object queryJSON(String sql);

	/**
	 * Creates a variable frame to support variable text functionality.
	 *
	 * @return The identifier or name of the newly created variable frame.
	 */
	String createVarFrame();
	
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	
	/*
	 * Too many compilation errors if we remove these things
	 * But we shouldn't use these anymore...
	 * 
	 */
	
	/**
	 * Returns an iterator over the data frame.
	 *
	 * @return An {@link IRawSelectWrapper} iterator for the data frame.
	 * @deprecated This method is deprecated and should not be used in new code.
	 */
	@Deprecated
	IRawSelectWrapper iterator();
	
	/**
	 * Gets all data from the data frame as a list of object arrays.
	 *
	 * @return A list where each element is an object array representing a row.
	 * @deprecated This method is deprecated and should not be used in new code.
	 */
	@Deprecated
	List<Object[]> getData();

	/**
	 * Gets the count of unique instances in the specified column.
	 *
	 * @param columnName The name of the column to count unique instances for.
	 * @return The number of unique instances in the specified column.
	 * @deprecated This method is deprecated and should not be used in new code.
	 */
	@Deprecated
	int getUniqueInstanceCount(String columnName);
	
	/*
	 * Damn... even older deprecated methods
	 */
	
//	/**
//	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
//	 * alter/modify the existing data-frame
//	 * @param routine				The IAnalytics routine to perform onto the data-frame
//	 */
//	@Deprecated
//	void performAnalyticTransformation(IAnalyticTransformationRoutine routine) throws RuntimeException;
//	
//	/**
//	 * Perform the inputed analytical routine onto the data frame. The routine does not necessarily have to 
//	 * alter/modify the existing data-frame
//	 * @param routine				The IAnalytics routine to perform onto the data-frame
//	 */
//	@Deprecated
//	void performAnalyticAction(IAnalyticActionRoutine routine) throws RuntimeException;
	
}
