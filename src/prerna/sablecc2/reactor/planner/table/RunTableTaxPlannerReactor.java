package prerna.sablecc2.reactor.planner.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.Translation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.TaxMapStore;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.TablePKSLPlanner;
import prerna.sablecc2.reactor.storage.StoreReactor;
import prerna.util.ArrayUtilityMethods;

public class RunTableTaxPlannerReactor extends AbstractTablePlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(RunTableTaxPlannerReactor.class.getName());
	
	private String scenarioHeader = "ProposalName"; //header of column containing Trump, House, etc
	private String aliasHeader = "Alias_1"; //header for value containing our column name
	private String valueHeader = "Value_1"; //header for value containing the value assigned to column name
	private String typeHeader = "Type_1";

	private TablePKSLPlanner originalPlan = null;

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
		long start = System.currentTimeMillis();

		// get the original version of the plan we want to save
		this.originalPlan = getPlanner();

		// now loop through and store all the necessary information
		// around each proposal
		Iterator<IHeadersDataRow> scenarioIterator = getIterator();
		// iterate through the scenario information
		// and generate a pksl planner for each scenario
		Map<String, TablePKSLPlanner> scenarioMap = getScenarioMap(scenarioIterator);
		
		// create the master return store
		// this will contain each scenario
		// pointing to another map
		// for that maps specific information
		InMemStore returnStore = new TaxMapStore();

		List<PKSLPlanner> planners = new ArrayList<>();
		for(String scenario : scenarioMap.keySet()) {
			LOGGER.info("Start execution for scenario = " + scenario);

			// create a new translation to run through
			Translation translation = new Translation();
			// get the planner for the scenario
			TablePKSLPlanner nextScenario = scenarioMap.get(scenario);
			nextScenario.addVariable("$Scenario", new NounMetadata(scenario, PkslDataTypes.CONST_STRING));
			translation.planner = nextScenario;
			
			// iterate through to determine execution order for
			// the scenario
			List<String> pkslList = collectRootPksls(nextScenario);
			while(!pkslList.isEmpty()) {
				PkslUtility.addPkslToTranslation(translation, pkslList);
//				updateTable(nextScenario, pkslList);
				pkslList = collectNextPksls(nextScenario);
			}
			
			resetTable(nextScenario);
			
			LOGGER.info("End execution for scenario = " + scenario);
			// after execution
			// we need to	 store the information
			LOGGER.info("Start storing data inside store");
			
			planners.add(translation.planner);
			
			InMemStore resultScenarioStore = new TaxMapStore();
			Set<String> variables = nextScenario.getVariables();
			for(String variable : variables) {
				try {
					NounMetadata noun = translation.planner.getVariableValue(variable);
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
			LOGGER.info("End storing data inside store");
			
		}
		
		long end = System.currentTimeMillis();
		System.out.println("****************    END RUN TAX PLANNER "+(end - start)+"ms      *************************");

		return new NounMetadata(planners, PkslDataTypes.PLANNER);
//		return new NounMetadata(returnStore, PkslDataTypes.IN_MEM_STORE);
	}

	/**
	 * Read the cached version of the planner 
	 * into a new planner for use within a scenario
	 * @return
	 */
	private TablePKSLPlanner getNewPlannerCopy() {
		
		TablePKSLPlanner newPlanner = new TablePKSLPlanner();
		newPlanner.setSimpleTable(this.originalPlan.getSimpleTable().copy());
		// now loop through the original planner
		// and set all the variables that are defined
		// into the new planner
		Set<String> variables = this.originalPlan.getVariables();
		for(String varName : variables) {
			NounMetadata varNoun = this.originalPlan.getVariable(varName);
			if(varNoun.isScalar()) {
//				System.out.println("Orig values ::: " + varName + " > " + this.originalPlan.getVariable(varName));
				newPlanner.addVariable(varName, varNoun);
			}
		}

		return newPlanner;
	}

	private Map<String, TablePKSLPlanner> getScenarioMap(Iterator<IHeadersDataRow> iterator) {
		// key is scenario, value is the map store for that scenario
		Map<String, TablePKSLPlanner> plannerMap = new HashMap<>();
		// define a central translation
		// to execute everything with
		// but substituting with the correct scenario planner
		PlannerTranslation translation = new PlannerTranslation();
		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();

			//TODO: move this outside so we don't calculate every time
			String[] headers = nextData.getHeaders();
			int scenarioHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, scenarioHeader);
			int aliasHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
			int valueHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, valueHeader);
			int typeHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, typeHeader);

			//grab each row
			Object[] values = nextData.getValues();

			//identify which scenario this is
			String scenario = values[scenarioHeaderIndex].toString();

			//grab alias and value (value should be literal, number, or column?)
			String alias = values[aliasHeaderIndex].toString();
			Object value = values[valueHeaderIndex];

			String type = values[typeHeaderIndex].toString();
			boolean isFormula = "formula".equalsIgnoreCase(type);

			//add to its specific scenario map store
			TablePKSLPlanner scenarioPlanner = null;
			if(plannerMap.containsKey(scenario)) {
				scenarioPlanner = plannerMap.get(scenario);
			} else {
				scenarioPlanner = getNewPlannerCopy();
				plannerMap.put(scenario, scenarioPlanner);
			}
			translation.planner = scenarioPlanner;
			// if it is a formula
			// parse and add to the scenario plan
			// else, add it as a variable
			if(isFormula) {
				String pkslString = PkslUtility.generatePKSLString(alias, value);
				PkslUtility.addPkslToTranslation(translation, pkslString);
			} else {
				scenarioPlanner.addVariable(alias, PkslUtility.getNoun(value));
			}
		}
		return plannerMap;
	}

	/****************************************************
	 * METHODS TO GRAB VALUES FROM REACTOR
	 ***************************************************/

	private Iterator<IHeadersDataRow> getIterator() {
		GenRowStruct allNouns = getNounStore().getNoun("PROPOSALS");
		Iterator<IHeadersDataRow> iterator = null;

		if(allNouns != null) {
			Job job = (Job)allNouns.get(0);
			iterator = job.getIterator();
		}
		return iterator;
	}

	private TablePKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		TablePKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (TablePKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return (TablePKSLPlanner)this.planner;
		}
	}
}
