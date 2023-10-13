package prerna.reactor.tax;
//package prerna.sablecc2.reactor.storage;
//
//import java.util.Hashtable;
//import java.util.Iterator;
//import java.util.Map;
//
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.impl.rdf.HeadersDataRow;
//import prerna.sablecc2.om.InMemStore;
//import prerna.sablecc2.om.NounMetadata;
//import prerna.sablecc2.om.TaxMapStore;
//
//public class TaxMapHeaderDataRowIterator implements Iterator<IHeadersDataRow>{
//
//	private TaxMapStore store = null;
//	private Iterator<String> keysIterator = null;
//	
//	public TaxMapHeaderDataRowIterator(TaxMapStore store) {
//		this.store = store;
//		this.keysIterator = store.getKeys().iterator();
//	}
//
//	@Override
//	public boolean hasNext() {
//		return keysIterator.hasNext();
//	}
//
//	@Override
//	public IHeadersDataRow next() {
//		// scenario name
//		String key = keysIterator.next();
//		// scenario in-mem map
//		InMemStore<String, NounMetadata> scenarioMap = (InMemStore<String, NounMetadata>)store.get(key).getValue();
//		
//		// loop through the scenario keys and flush out to map
//		Map<Object, Object> scenarioValues = new Hashtable<Object, Object>();
//		Iterator<String> scenarioKeysIt = scenarioMap.getKeys().iterator();
//		while(scenarioKeysIt.hasNext()) {
//			String scenarioKey = scenarioKeysIt.next();
//			scenarioValues.put(scenarioKey, scenarioMap.get(scenarioKey).getValue());
//		}
//		
//		String[] header = new String[]{key.toString()};
//		Object[] data = new Object[]{scenarioValues};
//		return new HeadersDataRow(header, data, data);
//	}
//	
//}
