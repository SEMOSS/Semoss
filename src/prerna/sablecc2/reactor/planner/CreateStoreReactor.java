package prerna.sablecc2.reactor.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.TaxMapStore;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.StoreReactor;

public class CreateStoreReactor extends AbstractReactor {

	private static final String STORE_NOUN = "store";
	private static final String KEY_NOUN = "key";
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	@Override
	public NounMetadata execute() {

		InMemStore returnStore = getStore();
		List<PKSLPlanner> planners = getPlanners();
		
		for(PKSLPlanner nextScenario : planners) {
			InMemStore resultScenarioStore = getStore();
			String scenario = nextScenario.getVariable("$Scenario").getValue().toString();
			Set<String> variables = nextScenario.getVariables();
			for(String variable : variables) {
				try {
					NounMetadata noun = nextScenario.getVariableValue(variable);
					if(noun.getNounName() != PkslDataTypes.CACHED_CLASS) {
						resultScenarioStore.put(variable, noun);
					}
				} catch(Exception e) {
//					System.out.println("Error with ::: " + variable);
				}
			}
	
			//add the result of the scenario as a inMemStore in our inMemStore we are returning
			returnStore.put(scenario, new NounMetadata(resultScenarioStore, PkslDataTypes.IN_MEM_STORE));
		}
		return new NounMetadata(returnStore, PkslDataTypes.IN_MEM_STORE);
	}
	
	private List<PKSLPlanner> getPlanners() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		List<PKSLPlanner> planners = new ArrayList<>(allNouns.size());
		if(allNouns != null) {
			
			for(int i = 0; i < allNouns.size(); i++) {
				Object nextNoun = allNouns.get(i);
				if(nextNoun instanceof List) {
					 List nounList = (List)nextNoun;
					 for(Object n : nounList) {
						 planners.add((PKSLPlanner)n);
					 }
				} else {
					planners.add((PKSLPlanner)nextNoun);
				}
			}
		}	
		return planners;
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
		
		else return new TaxMapStore();
	}
}
