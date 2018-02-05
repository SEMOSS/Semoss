package prerna.sablecc2.reactor.storage;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{

	private InMemStore<String, NounMetadata> store = null;
	private Iterator<String> keysIterator = null;
	
	public MapHeaderDataRowIterator(InMemStore store) {
		this.store = store;
		this.keysIterator = store.getKeys().iterator();
	}

	@Override
	public boolean hasNext() {
		return keysIterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		String key = keysIterator.next();
		NounMetadata value = store.get(key);
		
		String[] header = new String[]{key.toString()};
		Object[] data = new Object[]{value.getValue()};
		
		return new HeadersDataRow(header, data);
	}
	
}
