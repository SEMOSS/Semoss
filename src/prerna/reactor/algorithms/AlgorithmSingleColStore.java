package prerna.reactor.algorithms;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class AlgorithmSingleColStore<T> implements Iterator<IHeadersDataRow> {

	private Map<Object, T> store = new Hashtable<Object, T>();
	private String[] headers;
	private SemossDataType[] types;
	
	// for iterating
	private boolean init = false;
	private Iterator<Object> keys;
	
	public void put(Object key, T value) {
		this.store.put(key, value);
	}
	
	public T get(Object key) {
		return this.store.get(key);
	}
	
	public boolean containsKey(Object key) {
		return this.store.containsKey(key);
	}
	
	public void init() {
		init = true;
		this.keys = this.store.keySet().iterator();
	}
	
	public void setHeaders(String[] headers) {
		this.headers = headers;
	}
	
	public void setTypes(SemossDataType[] types) {
		this.types = types;
	}
	
	@Override
	public boolean hasNext() {
		if(!this.init) {
			init();
		}
		return this.keys.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		if(!this.init) {
			init();
		}
		Object key = this.keys.next();
		Object[] values = new Object[]{key, this.store.get(key)};
		// create the HeaderDataRow
		IHeadersDataRow row = new HeadersDataRow(this.headers, values);
		return row;
	}
}
