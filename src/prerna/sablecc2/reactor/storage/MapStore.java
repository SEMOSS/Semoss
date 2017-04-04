package prerna.sablecc2.reactor.storage;

import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class MapStore extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(MapStore.class.getName());

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	@Override
	public Object execute()
	{
		// create a new empty map for storage
		NounMetadata storeVar = new NounMetadata(new Hashtable<String, Object>(), PkslDataTypes.IN_MEM_STORE);
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
	
	
}
