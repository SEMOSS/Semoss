package prerna.reactor.tax;
//package prerna.sablecc2.reactor.storage;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.InMemStore;
//import prerna.sablecc2.om.NounMetadata;
//import prerna.sablecc2.om.PkslDataTypes;
//import prerna.sablecc2.om.TaxMapStore;
//import prerna.sablecc2.reactor.AbstractReactor;
//import prerna.sablecc2.reactor.PKSLPlanner;
//
//public class TaxRetrieveValue extends AbstractReactor {
//
//	private static final Logger LOGGER = LogManager.getLogger(TaxRetrieveValue.class.getName());
//
//	//TODO: find a common place to put these
//	private static final String STORE_NOUN = "store";
//	private static final String KEY_NOUN = "key";
//	
//	/**
//	 * This reactor takes in 2 nouns
//	 * store -> this points to the store name
//	 * 			this will automatically be replaced with
//	 * 			the NounMetadata that is in the pkslplanner
//	 * 
//	 * key ->	the key that the value is stored under
//	 */
//	@Override
//	public NounMetadata execute()
//	{
//		InMemStore storeVariable = getStore();
//		List<PKSLPlanner> planners = getPlanners();
//		Set<String> scenarioKeys = storeVariable.getKeys();
//		
//		// need to make a new InMemStore
//		// that will contain a reference to each scenario
//		// and another store of its values
//		InMemStore retStoreVar = null;
//		try {
//			retStoreVar = storeVariable.getClass().newInstance();
//		} catch (InstantiationException | IllegalAccessException e) {
//			e.printStackTrace();
//		}
//		
//		for(PKSLPlanner planner : planners) {
//			
//			String scenarioName = planner.getVariable("$SCENARIO").getValue().toString();
//			
//			// need to make a new InMemStore
//			// for each scenario to hold the subportion
//			// of data to send back
//			InMemStore newScenarioStore = null;
//			try {
//				newScenarioStore = storeVariable.getClass().newInstance();
//			} catch (InstantiationException | IllegalAccessException e) {
//				e.printStackTrace();
//			}
//			
//			GenRowStruct grs = this.store.getNoun(KEY_NOUN);
//			int numGrs = grs.size();
//
//			for(int i = 0; i < numGrs; i++) {
//				String key = grs.get(i).toString();
//				// we will append the subset of info into it
//				try {
//					NounMetadata noun = planner.getVariableValue(key);
//					if(noun.getNounName() != PkslDataTypes.CACHED_CLASS) {
//						newScenarioStore.put(key, noun);
//					}
//				} catch(Exception e) {
//					
//				}
//			}
//			
//			retStoreVar.put(scenarioName.toString(), new NounMetadata(newScenarioStore, PkslDataTypes.IN_MEM_STORE));
//		}
//			
//		return new NounMetadata(retStoreVar, PkslDataTypes.IN_MEM_STORE);
//	}
//	
//	private InMemStore getStore() {
//		// could be passed directly in the method -> as store
//		GenRowStruct storeGrs = this.store.getNoun(STORE_NOUN);
//		if(storeGrs != null) {
//			return (InMemStore) storeGrs.get(0);
//		}
//		
//		// could be passed as a $RESULT -> as STORE
//		storeGrs = this.store.getNoun(PkslDataTypes.IN_MEM_STORE.toString());
//		if(storeGrs != null) {
//			return (InMemStore) storeGrs.get(0);
//		}
//		
//		// see if there is anything in curRow with store
//		List<NounMetadata> passedResults = this.curRow.getNounsOfType(PkslDataTypes.IN_MEM_STORE);
//		if(passedResults != null && !passedResults.isEmpty()) {
//			return (InMemStore) passedResults.get(0).getValue();
//		}
//		
//		else return new TaxMapStore();
//	}
//	
//	private List<PKSLPlanner> getPlanners() {
//		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
//		List<PKSLPlanner> planners = new ArrayList<>(allNouns.size());
//		if(allNouns != null) {
//			
//			for(int i = 0; i < allNouns.size(); i++) {
//				Object nextNoun = allNouns.get(i);
//				if(nextNoun instanceof List) {
//					 List nounList = (List)nextNoun;
//					 for(Object n : nounList) {
//						 planners.add((PKSLPlanner)n);
//					 }
//				} else {
//					planners.add((PKSLPlanner)nextNoun);
//				}
//			}
//		}	
//		return planners;
//	}
//	
//	@Override
//	public List<NounMetadata> getOutputs() {
//		// output is the signature
//		List<NounMetadata> outputs = new Vector<NounMetadata>();
//		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.LAMBDA);
//		outputs.add(output);
//		return outputs;
//	}
//}
