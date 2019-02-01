package prerna.sablecc2.reactor.storage;

import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapHeaderDataRowIterator implements IRawSelectWrapper {

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
	
	@Override
	public long getNumRows() {
		return store.getKeys().size();
	}

	@Override
	public long getNumRecords() {
		return store.getKeys().size();
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setEngine(IEngine engine) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getHeaders() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SemossDataType[] getTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
}
