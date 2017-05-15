package prerna.sablecc2.reactor.storage;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.TaxMapStore;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.reactor.AbstractReactor;

public class StoreReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(StoreReactor.class.getName());
	private static final String STORE_NOUN = "store";
	

	@Override
	public NounMetadata execute() {
		// create a new empty map for storage
		InMemStore store = getStore();
		NounMetadata storeVar = new NounMetadata(store, PkslDataTypes.IN_MEM_STORE);
		return storeVar;
	}
	
	private InMemStore getStore() {
		String storeType = getStoreType();
		if("tax".equalsIgnoreCase(storeType)) {
			return new TaxMapStore();
		} else if("var".equalsIgnoreCase(storeType)) {
			return new VarStore();
		} else {
			return new VarStore();
		}
	}
	
	private String getStoreType() {
		GenRowStruct storeTypeRow = this.store.getNoun(STORE_NOUN);
		String storeType = "";
		if(storeTypeRow != null) {
			storeType = storeTypeRow.get(0).toString();
		}
		return storeType;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.IN_MEM_STORE);
		outputs.add(output);
		return outputs;
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
