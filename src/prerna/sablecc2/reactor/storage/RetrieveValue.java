package prerna.sablecc2.reactor.storage;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
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
	public NounMetadata execute()
	{
		NounMetadata storeNoun = (NounMetadata) this.store.getNoun(STORE_NOUN).getNoun(0);
		InMemStore storeVariable = (InMemStore) storeNoun.getValue();
		
		GenRowStruct grs = this.store.getNoun(KEY_NOUN);
		int numGrs = grs.size();
		// if there is only one return
		// we send back the actual value
		if(numGrs == 1) {
			Object key = grs.get(0);
			NounMetadata valueData = storeVariable.get(key);
			return valueData;
		} 
		// if there are multiple returns
		// we will create a new InMemStore
		// with only the subset that is defined
		else {
			// need to make a new InMemStore
			// we will make a new one of the same type of the current class
			InMemStore retStoreVar = null;
			try {
				retStoreVar = storeVariable.getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
			for(int i = 0; i < numGrs; i++) {
				Object key = grs.get(i);
				// we will append the subset of info into it
				retStoreVar.put(key, storeVariable.get(key));
			}
			
			return new NounMetadata(retStoreVar, PkslDataTypes.IN_MEM_STORE);
		}
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
