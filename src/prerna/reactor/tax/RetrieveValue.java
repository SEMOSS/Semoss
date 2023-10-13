package prerna.reactor.tax;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrieveValue extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(RetrieveValue.class);

	//TODO: find a common place to put these
	private static final String STORE_NOUN = "store";
	private static final String KEY_NOUN = "key";
	
	/**
	 * This reactor takes in 2 nouns
	 * store -> this points to the store name
	 * 			this will automatically be replaced with
	 * 			the NounMetadata that is in the pixel planner
	 * 
	 * key ->	the key that the value is stored under
	 */

	@Override
	public NounMetadata execute()
	{
		InMemStore storeVariable = getStore();
		
		GenRowStruct grs = this.store.getNoun(KEY_NOUN);
		int numGrs = grs.size();
		// if there is only one return
		// we send back the actual value
		if(numGrs == 1) {
			Object key = grs.get(0);
			NounMetadata valueData = (NounMetadata)storeVariable.get(key);
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
				logger.error("StackTrace: ", e);
			}

			if (retStoreVar != null) {
				for(int i = 0; i < numGrs; i++) {
					Object key = grs.get(i);
					// we will append the subset of info into it
					retStoreVar.put(key, storeVariable.get(key));
				}
			}
			
			return new NounMetadata(retStoreVar, PixelDataType.IN_MEM_STORE);
		}
	}
	
	private InMemStore getStore() {
		// could be passed directly in the method -> as store
		GenRowStruct storeGrs = this.store.getNoun(STORE_NOUN);
		if(storeGrs != null) {
			return (InMemStore) storeGrs.get(0);
		}
		
		// could be passed as a $RESULT -> as STORE
		storeGrs = this.store.getNoun(PixelDataType.IN_MEM_STORE.getKey());
		if(storeGrs != null) {
			return (InMemStore) storeGrs.get(0);
		}
		
		// see if there is anything in curRow with store
		List<NounMetadata> passedResults = this.curRow.getNounsOfType(PixelDataType.IN_MEM_STORE);
		if(passedResults != null && !passedResults.isEmpty()) {
			return (InMemStore) passedResults.get(0).getValue();
		}
		
		// out of options, throw an error
		throw new IllegalArgumentException("Could not find store to retrieve values from");
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// output is the signature
		List<NounMetadata> outputs = new Vector<>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.LAMBDA);
		outputs.add(output);
		return outputs;
	}
}
