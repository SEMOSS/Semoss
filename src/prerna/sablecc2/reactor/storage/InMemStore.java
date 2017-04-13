package prerna.sablecc2.reactor.storage;

import java.util.Iterator;
import java.util.Set;

import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.NounMetadata;

public interface InMemStore {

	/**
	 * Default iterator
	 * @return
	 */
	Iterator<IHeadersDataRow> getIterator();
	
	/**
	 * Iterator with qs defined
	 * @param qs
	 * @return
	 */
	Iterator<IHeadersDataRow> getIterator(QueryStruct2 qs);

	/**
	 * Insert data to be stored
	 * @param key
	 * @param val
	 */
	void put(Object key, NounMetadata value);
	
	/**
	 * Get data that is stored
	 * @param key
	 * @return
	 */
	NounMetadata get(Object key);
	
	/**
	 * Remove data that is stored
	 * @param key
	 */
	void remove(Object key);

	/**
	 * Get the set of keys currently stored
	 * @return
	 */
	Set<Object> getStoredKeys();
}
