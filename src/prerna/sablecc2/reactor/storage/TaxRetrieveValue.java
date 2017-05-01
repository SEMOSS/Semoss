package prerna.sablecc2.reactor.storage;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class TaxRetrieveValue extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(TaxRetrieveValue.class.getName());

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
	public NounMetadata execute()
	{
		InMemStore storeVariable = getStore();
		Set<Object> scenarioKeys = storeVariable.getStoredKeys();
		
		// need to make a new InMemStore
		// that will contain a reference to each scenario
		// and another store of its values
		InMemStore retStoreVar = null;
		try {
			retStoreVar = storeVariable.getClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		
		for(Object scenarioName : scenarioKeys) {
			// need to make a new InMemStore
			// for each scenario to hold the subportion
			// of data to send back
			InMemStore newScenarioStore = null;
			try {
				newScenarioStore = storeVariable.getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
			
			GenRowStruct grs = this.store.getNoun(KEY_NOUN);
			int numGrs = grs.size();
			InMemStore existingScenarioStore = (InMemStore) storeVariable.get(scenarioName).getValue();

			for(int i = 0; i < numGrs; i++) {
				Object key = grs.get(i);
				// we will append the subset of info into it
				newScenarioStore.put(key, existingScenarioStore.get(key));
			}
			
			retStoreVar.put(scenarioName.toString(), new NounMetadata(newScenarioStore, PkslDataTypes.IN_MEM_STORE));
		}
			
		return new NounMetadata(retStoreVar, PkslDataTypes.IN_MEM_STORE);
	}
	
	private InMemStore getStore() {
		// could be passed directly in the method -> as store
		GenRowStruct storeGrs = this.store.getNoun(STORE_NOUN);
		if(storeGrs != null) {
			return (InMemStore) storeGrs.get(0);
		}
		
		// could be passed as a $RESULT -> as STORE
		storeGrs = this.store.getNoun(PkslDataTypes.IN_MEM_STORE.toString());
		if(storeGrs != null) {
			return (InMemStore) storeGrs.get(0);
		}
		
		// see if there is anything in curRow with store
		List<NounMetadata> passedResults = this.curRow.getNounsOfType(PkslDataTypes.IN_MEM_STORE);
		if(passedResults != null && !passedResults.isEmpty()) {
			return (InMemStore) passedResults.get(0).getValue();
		}
		
		// out of options, throw an error
		throw new IllegalArgumentException("Could not find store to retrieve values from");
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// output is the signature
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.LAMBDA);
		outputs.add(output);
		return outputs;
	}
}
