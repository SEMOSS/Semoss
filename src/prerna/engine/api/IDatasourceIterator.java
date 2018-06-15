package prerna.engine.api;

import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

public interface IDatasourceIterator extends Iterator<IHeadersDataRow> {

	/**
	 * This method needs to be called to actually run the query
	 */
	void execute();
	
	/**
	 * Set the query
	 * @param query
	 */
	void setQuery(String query);
	
	/**
	 * Clean up the data source
	 */
	void cleanUp();
	
	/**
	 * Get the names of the returns
	 */
	String[] getHeaders();
	
	/**
	 * Get the types for each return
	 */
	SemossDataType[] getTypes();
	
	/**
	 * Get the size of the return
	 */
	long getNumRecords();
	
	/**
	 * Reset the iterator
	 */
	void reset();
	
}
