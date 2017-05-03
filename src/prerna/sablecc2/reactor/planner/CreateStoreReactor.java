package prerna.sablecc2.reactor.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;
import prerna.sablecc2.reactor.storage.TaxMapStore;

public class CreateStoreReactor extends AbstractReactor {

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

		InMemStore returnStore = new TaxMapStore();
		List<PKSLPlanner> planners = getPlanners();
		
		for(PKSLPlanner nextScenario : planners) {
			InMemStore resultScenarioStore = new MapStore();
			String scenario = nextScenario.getVariable("$Scenario").getValue().toString();
			Set<String> variables = nextScenario.getVariables();
			for(String variable : variables) {
				try {
					NounMetadata noun = nextScenario.getVariableValue(variable);
					if(noun.getNounName() != PkslDataTypes.CACHED_CLASS) {
						resultScenarioStore.put(variable, noun);
					}
				} catch(Exception e) {
					e.printStackTrace();
					System.out.println("Error with ::: " + variable);
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

}
