package prerna.engine.api;

import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;

public interface IRawSelectWrapper extends IEngineWrapper, Iterator<IHeadersDataRow> {

	/**
	 * Get the names of the returns
	 */
	String[] getHeaders();
	
	//TODO: move to pixel data type
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
