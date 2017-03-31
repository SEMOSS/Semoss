package prerna.sablecc2.reactor.storage;

import java.util.Hashtable;

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
	protected void mergeUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void updatePlan() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Object execute()
	{
		// create a new empty map for storage
		NounMetadata storeVar = new NounMetadata(new Hashtable<String, Object>(), PkslDataTypes.IN_MEM_STORE);
		return storeVar;
	}
}
