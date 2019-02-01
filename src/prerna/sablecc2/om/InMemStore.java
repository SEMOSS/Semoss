package prerna.sablecc2.om;

import java.util.Set;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;

public interface InMemStore<K, V> {

	/**
	 * Default iterator
	 * @return
	 */
	public IRawSelectWrapper getIterator();
	
	/**
	 * Iterator with qs defined
	 * @param qs
	 * @return
	 */
	public IRawSelectWrapper getIterator(SelectQueryStruct qs);

	/**
	 * Insert data to be stored
	 * @param key
	 * @param val
	 */
	public void put(K key, V value);
	
	/**
	 * Get data that is stored
	 * @param key
	 * @return
	 */
	public V get(K key);
	
	/**
	 * 
	 * @param gets the evaluated value of the data stored in the key
	 * @return
	 */
	public V getEvaluatedValue(K key);
	
	/**
	 * Remove data that is stored
	 * @param key
	 * @return 
	 */
	public V remove(K key);

	/**
	 * Returns whether the key exists in the mem store
	 * @param key
	 * @return
	 */
	public boolean containsKey(K key);
	/**
	 * Get the set of keys currently stored
	 * @return
	 */
	public Set<K> getKeys();
	
	/**
	 * clears the object of all keys and values
	 */
	public void clear();
}
