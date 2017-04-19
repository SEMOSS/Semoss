package prerna.sablecc2.reactor.storage;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class MapStore extends AbstractReactor implements InMemStore {

	private static final Logger LOGGER = LogManager.getLogger(MapStore.class.getName());

	private Map<Object, NounMetadata> thisStore = new Hashtable<Object, NounMetadata>();
	
	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	@Override
	public NounMetadata execute()
	{
		// create a new empty map for storage
		NounMetadata storeVar = new NounMetadata(this, PkslDataTypes.IN_MEM_STORE);
		return storeVar;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		// this is a shallow reactor
		// it generates a new map
		// but its the responsibility of 
		// a parent reactor
		// like the assignment reactor
		// to actually store the map into the planner
		// as a variable for future use
		return null;
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		// this is a shallow reactor
		// it generates a new map
		// but its the responsibility of 
		// a parent reactor
		// like the assignment reactor
		// to actually store the map into the planner
		// as a variable for future use
		return null;
	}

	@Override
	public Iterator<IHeadersDataRow> getIterator() {
		return new MapHeaderDataRowIterator(this);
	}

	@Override
	public Iterator<IHeadersDataRow> getIterator(QueryStruct2 qs) {
		// TODO how does QS fit a map???
		return getIterator();
	}

	@Override
	public void put(Object key, NounMetadata value) {
		LOGGER.debug("Storing ::: " + key + " with value ( " + value + " )");
		thisStore.put(key, value);
	}

	@Override
	public NounMetadata get(Object key) {
		NounMetadata retNoun = thisStore.get(key);
		LOGGER.debug("Retrieving ::: " + key + " with value ( " + retNoun + " )");
		return retNoun;
	}

	@Override
	public void remove(Object key) {
		thisStore.remove(key);
	}
	
	@Override
	public Set<Object> getStoredKeys() {
		return thisStore.keySet();
	}
	
}
