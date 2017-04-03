package prerna.sablecc2.reactor.storage;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RetrieveValue extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(RetrieveValue.class.getName());

	//TODO: find a common place to put these
	private static final String STORE_NOUN = "store";
	private static final String KEY_NOUN = "key";
	
	/**
	 * This reactor takes in 2 nouns
	 * store -> this points to the store name
	 * 			this will automatically be replaced with
	 * 			the NounMetadata that is in the pkslplanner
	 * 
	 * key ->	the key that the value is stored under
	 */
	
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
		NounMetadata storeNoun = (NounMetadata) this.store.getNoun(STORE_NOUN).getNoun(0);
		String key = this.store.getNoun(KEY_NOUN).get(0).toString();
		
		// when we update the store noun
		// it will update the reference for all future use
	
		// TODO: build a common interface when we get different types of IN_MEM storage data structures
		Object storeVariable = storeNoun.getValue();
		
		NounMetadata valueData = null;
		
		if(storeVariable instanceof Map) {
			valueData = (NounMetadata) ((Map) storeVariable).get(key);
			LOGGER.info("Found value = " + valueData + " from key = " + key);
		} else {
			throw new IllegalArgumentException("Unable to get value in store = " + store + " with key = " + key);
		}
		
		return valueData;
	}
}
