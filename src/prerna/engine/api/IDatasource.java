package prerna.engine.api;

import prerna.query.querystruct.SelectQueryStruct;

public interface IDatasource {

	/**
	 * Get a select iterator for a given query
	 * @return
	 */
	IDatasourceIterator getIterator(String query);
	
	/**
	 * Get the iterator that we will set
	 * @return
	 */
	IDatasourceIterator getIterator(SelectQueryStruct qs);
	
}
