package prerna.engine.api;

import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;

public interface IRawSelectWrapper extends IEngineWrapper, Iterator<IHeadersDataRow> {

	/**
	 * Get the names of the returns
	 */
	String[] getHeaders();
	
	/**
	 * Get the types for each return
	 */
	//TODO: move to pixel data type
	SemossDataType[] getTypes();

	/**
	 * Get the number of rows
	 * @return
	 */
	long getNumRows() throws Exception;
	
	/**
	 * Get the size of the return
	 */
	long getNumRecords() throws Exception;
	
	/**
	 * Reset the iterator
	 */
	void reset() throws Exception;
	
	/**
	 * Can the full result set be flushed directly from the object
	 * @return
	 */
	boolean flushable();
	
	/**
	 * Return data flushed as a string 
	 * @return
	 */
	String flush();
}
