package prerna.algorithm.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public interface ITableDataFrame extends IDataMaker {
	
	/**
	 * Adds a row to the data-frame
	 * @param rowCleanData			The array of clean values where indices match the columns in the data-frame
	 * @param headers				The headers corresponding to the new row to add
	 */
	void addRow(Object[] cleanCells, String[] headers);
	
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
	 * Get the clean column frame headers
	 * @return
	 */
	String[] getColumnHeaders();
	
	/**
	 * Get the qs names for the data-frame
	 * @return						The column header names for the data-frame
	 */
	String[] getQsHeaders();
	
	/**
	 * 	 * Returns the iterator that will iterate through a numeric column
	 * the iterator will return the each unique value in the column as a function -> x' = (x - min(columnHeader))/(max(columnHeader) - min(columnHeader))	 * @param columnHeader
	 * @return
	 */
	Iterator<List<Object[]>> scaledUniqueIterator(String uniqueHeaderName, List<String> attributeUniqueHeaderName);
	
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
	 * Persist a filter on the frame
	 * @param filter
	 */
	void addFilter(GenRowFilters filter);

	/**
	 * Add a filter to the frame
	 * @param filter
	 */
	void addFilter(IQueryFilter filter);
	
	/**
	 * Persist a filter on the frame
	 * Set will override any existing filter on the frame for a given column
	 * @param filter
	 */
	void setFilter(GenRowFilters filter);

	/**
	 * Get the filters on the frame
	 * @return
	 */
	GenRowFilters getFrameFilters();
	
	/**
	 * Set a brand new frame filters object
	 */
	void setFrameFilters(GenRowFilters filter);
	
	/**
	 * Unfilter all values for the passed in column header
	 * @param columnHeader			The column header to remove the filter on
	 * @return						The data-frame with the filtering applied
	 */
	boolean unfilter(String columnHeader);

	/**
	 * Unfilter all columns for the data frame
	 */
	boolean unfilter();
	
	/**
	 * Removes a column from the data frame
	 * @param columnHeader			The column header to remove from the data-frame
	 */
	void removeColumn(String columnHeader);
	
	/**
	 * Returns if the ITable is empty
	 * @return
	 */
	boolean isEmpty();
	
	/**
	 * Serialize the dataframe
	 * @param folderDir
	 * @throws IOException 
	 */
	CachePropFileFrameObject save(String folderDir) throws IOException;
	
	/**
	 * Deserialize the dataframe
	 * @param cf
	 * @return
	 * @throws IOException 
	 */
	void open(CachePropFileFrameObject cf) throws IOException;

	/**
	 * Get the number of rows for the frame
	 * @param tableName
	 * @return
	 */
	long size(String tableName);
	
	IRawSelectWrapper query(String query);
	
	IRawSelectWrapper query(SelectQueryStruct qs);
	
	/**
	 * Get the name of the frame
	 */
	String getName();
	
	/**
	 * Set the name of the frame
	 * @param name
	 */
	void setName(String name);
	
	OwlTemporalEngineMeta getMetaData();

	void setMetaData(OwlTemporalEngineMeta metaData);

	void syncHeaders();
	
	void setLogger(Logger logger);
	
	/**
	 * Used to delete the frame
	 */
	void close();
	
	//////////////////////////////////////////////////
	
	// Info that is cached on the frame
	
	/**
	 * Is the column unique within the frame
	 * TODO: this is assuming your column is part of a table
	 * note - even if native frame with joins, the result set 
	 * where this column is returned is still part of a single table
	 * @param columnName
	 * @return
	 */
	Boolean isUniqueColumn(String columnName);
	
	/**
	 * Clear any cached information on the frame since it
	 * is no longer valid
	 */
	void clearCachedInfo();
	
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	
	/*
	 * Too many compilation errors if we remove these things
	 * But we shoudln't use these anymore...
	 * 
	 */
	
	@Deprecated
	IRawSelectWrapper iterator();
	
	@Deprecated
	List<Object[]> getData();

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
