package prerna.sablecc2.reactor.storage;

import java.util.List;
import java.util.Map;
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
		PKSLPlanner planner = getPlanner();
		Class<BaseJavaRuntime> javaClass = (Class<BaseJavaRuntime>) planner.getProperty("RUN_CLASS", "RUN_CLASS");
		BaseJavaRuntime javaRunClass = null;
		try {
			javaRunClass = javaClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		javaRunClass.execute();
		// for each scenario to hold the subportion
		// of data to send back
		InMemStore retStoreVar = new VarStore();

		GenRowStruct grs = this.store.getNoun(KEY_NOUN);
		int numGrs = grs.size();

		// go through and get all the aliases the FE want
		List<String> aliases = new Vector<String>();
		for (int i = 0; i < numGrs; i++) {
			String key = grs.get(i).toString();
			aliases.add(key);
		}
		
		// convert the alias to the hashcode which was used in execution
		Map<String, String> aliasHashMap = TaxUtility.mapAliasToHash(aliases);
		
		for(String alias : aliasHashMap.keySet()) {
			String hashcode = aliasHashMap.get(alias);
			Object value =  javaRunClass.getVariables().get(hashcode);
			// return alias to the value associated with the hash
			retStoreVar.put(alias, new NounMetadata(value, PkslDataTypes.CONST_STRING));
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
}
