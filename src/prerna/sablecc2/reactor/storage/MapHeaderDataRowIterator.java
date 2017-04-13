package prerna.sablecc2.reactor.storage;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.sablecc2.om.NounMetadata;

public class MapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{

	private MapStore store = null;
	private Iterator<Object> keysIterator = null;
	
	public MapHeaderDataRowIterator(MapStore store) {
		this.store = store;
		this.keysIterator = store.getStoredKeys().iterator();
	}

	@Override
	public boolean hasNext() {
		return keysIterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		Object key = keysIterator.next();
		NounMetadata value = store.get(key);
		
		String[] header = new String[]{key.toString()};
		Object[] data = new Object[]{value.getValue()};
		
		return new HeadersDataRow(header, data, data);
	}
	
}
