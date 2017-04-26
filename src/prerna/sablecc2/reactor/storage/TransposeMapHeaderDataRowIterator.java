package prerna.sablecc2.reactor.storage;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;

public class TransposeMapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{

	private boolean hasNext = true;
	private MapStore store = null;
	private String[] headers;
	
	public TransposeMapHeaderDataRowIterator(MapStore store) {
		this.store = store;
		headers = store.getStoredKeys().toArray(new String[0]);
	}
	
	public TransposeMapHeaderDataRowIterator(MapStore store, String[] headers) {
		this.store = store;
		this.headers = headers;
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public IHeadersDataRow next() {
		Object[] data = new Object[headers.length];
		for(int i = 0; i < headers.length; i++) {
			data[i] = store.get(headers[i]).getValue();
		}
		
		hasNext = false;
		return new HeadersDataRow(headers, data, data);
	}
}
