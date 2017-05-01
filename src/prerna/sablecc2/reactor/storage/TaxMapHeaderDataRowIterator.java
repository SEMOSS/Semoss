package prerna.sablecc2.reactor.storage;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;

public class TaxMapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{

	private MapStore store = null;
	private Iterator<Object> keysIterator = null;
	
	public TaxMapHeaderDataRowIterator(MapStore store) {
		this.store = store;
		this.keysIterator = store.getStoredKeys().iterator();
	}

	@Override
	public boolean hasNext() {
		return keysIterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// scenario name
		Object key = keysIterator.next();
		// scenario in-mem map
		InMemStore scenarioMap = (InMemStore) store.get(key).getValue();
		
		// loop through the scenario keys and flush out to map
		Map<Object, Object> scenarioValues = new Hashtable<Object, Object>();
		Iterator<Object> scenarioKeysIt = scenarioMap.getStoredKeys().iterator();
		while(scenarioKeysIt.hasNext()) {
			Object scenarioKey = scenarioKeysIt.next();
			scenarioValues.put(scenarioKey, scenarioMap.get(scenarioKey).getValue());
		}
		
		String[] header = new String[]{key.toString()};
		Object[] data = new Object[]{scenarioValues};
		return new HeadersDataRow(header, data, data);
	}
	
}
