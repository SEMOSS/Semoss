package prerna.sablecc2.reactor.storage;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.PKSLPlanner;

public class TaxRetrieveValue2 extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(TaxRetrieveValue2.class.getName());

	// TODO: find a common place to put these
//	private static final String STORE_NOUN = "store";
	private static final String KEY_NOUN = "key";

	/**
	 * This reactor takes in 2 nouns store -> this points to the store name this
	 * will automatically be replaced with the NounMetadata that is in the
	 * pkslplanner
	 * 
	 * key -> the key that the value is stored under
	 */
	@Override
	public NounMetadata execute() {
//		InMemStore storeVariable = getStore();
		PKSLPlanner planner = getPlanner();
		BaseJavaRuntime javaRunClass = (BaseJavaRuntime) planner.getProperty("RUN_CLASS", "RUN_CLASS");
		javaRunClass.execute();
		// for each scenario to hold the subportion
		// of data to send back
		InMemStore retStoreVar = new VarStore();
//		try {
//			retStoreVar = storeVariable.getClass().newInstance();
//		} catch (InstantiationException | IllegalAccessException e) {
//			e.printStackTrace();
//		}

		GenRowStruct grs = this.store.getNoun(KEY_NOUN);
		int numGrs = grs.size();

		for (int i = 0; i < numGrs; i++) {
			String key = grs.get(i).toString();
			// we will append the subset of info into it
			Object value =  javaRunClass.getVariables().get(key);
			retStoreVar.put(key, new NounMetadata(value, PkslDataTypes.CONST_STRING));
		}
		for(Object objKey : retStoreVar.getKeys()){
			System.out.println(objKey.toString()+" : "+ retStoreVar.get(objKey).toString());
		}
		return new NounMetadata(retStoreVar, PkslDataTypes.IN_MEM_STORE);

	}

	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if (allNouns != null && !allNouns.isEmpty()) {
			planner = (PKSLPlanner) allNouns.get(0);
		}
		return planner;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		// output is the signature
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.LAMBDA);
		outputs.add(output);
		return outputs;
	}
	
//	private InMemStore getStore() {
//		// could be passed directly in the method -> as store
//		GenRowStruct storeGrs = this.store.getNoun(STORE_NOUN);
//		if (storeGrs != null) {
//			return (InMemStore) storeGrs.get(0);
//		}
//
//		// could be passed as a $RESULT -> as STORE
//		storeGrs = this.store.getNoun(PkslDataTypes.IN_MEM_STORE.toString());
//		if (storeGrs != null) {
//			return (InMemStore) storeGrs.get(0);
//		}
//
//		// see if there is anything in curRow with store
//		List<NounMetadata> passedResults = this.curRow.getNounsOfType(PkslDataTypes.IN_MEM_STORE);
//		if (passedResults != null && !passedResults.isEmpty()) {
//			return (InMemStore) passedResults.get(0).getValue();
//		}
//
//		else
//			return new TaxMapStore();
//	}
}
