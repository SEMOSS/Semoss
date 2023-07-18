package prerna.om;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TransposeMapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{

	private boolean hasNext = true;
	private InMemStore<String, NounMetadata> store = null;
	private String[] headers;
	
	public TransposeMapHeaderDataRowIterator(InMemStore store) {
		this.store = store;
		headers = this.store.getKeys().toArray(new String[0]);
	}
	
	public TransposeMapHeaderDataRowIterator(InMemStore store, String[] headers) {
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
		return new HeadersDataRow(headers, data);
	}
}
