package prerna.sablecc2.reactor.planner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.Translation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;
import prerna.sablecc2.reactor.storage.TaxMapStore;
import prerna.util.ArrayUtilityMethods;
import prerna.util.MyGraphIoRegistry;

public class RunTaxPlannerReactor2 extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(RunTaxPlannerReactor2.class.getName());
	
	private static int fileCount = 0;
	private String scenarioHeader = "ProposalName"; //header of column containing Trump, House, etc
	private String aliasHeader = "Alias_1"; //header for value containing our column name
	private String valueHeader = "Value_1"; //header for value containing the value assigned to column name
	private String typeHeader = "Type_1";

	private PKSLPlanner originalPlan = null;
	private String fileName;

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
		// remove this stupid thing!!!
		this.originalPlan.g.traversal().V().has(PKSLPlanner.TINKER_ID, "OP:FRAME").drop().iterate();
		// save the location
		this.fileName = getFileName();
		saveGraph(this.originalPlan, this.fileName);

		// now loop through and store all the necessary information
		// around each proposal
		Iterator<IHeadersDataRow> scenarioIterator = getIterator();
		// iterate through the scenario information
		// and generate a pksl planner for each scenario
		Map<String, PKSLPlanner> scenarioMap = getScenarioMap(scenarioIterator);
		
		// create the master return store
		// this will contain each scenario
		// pointing to another map
		// for that maps specific information
		InMemStore returnStore = new TaxMapStore();

		for(String scenario : scenarioMap.keySet()) {
			LOGGER.info("Start execution for scenario = " + scenario);

			// create a new translation to run through
			Translation translation = new Translation();
			// get the planner for the scenario
			PKSLPlanner nextScenario = scenarioMap.get(scenario);
			translation.planner = nextScenario;
			
			// iterate through to determine execution order for
			// the scenario
			List<String> pkslList = getPksls(nextScenario);
			
			PkslUtility.addPkslToTranslation(translation, pkslList);
			
			LOGGER.info("End execution for scenario = " + scenario);
			// after execution
			// we need to	 store the information
			LOGGER.info("Start storing data inside store");
			
			InMemStore resultScenarioStore = new MapStore();
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

		return new NounMetadata(returnStore, PkslDataTypes.IN_MEM_STORE);
	}

	private List<String> getPksls(PKSLPlanner planner) {
		// keep track of all the pksls to execute
		List<String> pksls = new Vector<String>();

		// get the list of the root vertices
		// these are the vertices we can run right away
		// and are the starting point for the plan execution
		Set<Vertex> rootVertices = getRootPksls(planner);
		// using the root vertices
		// iterate down all the other vertices and add the signatures
		// for the desired travels in the appropriate order
		// note: this is adding to the list of undefined variables
		// calculated at beginning of class 
		traverseDownstreamVertsAndOrderProcessing(rootVertices, pksls);
		return pksls;
	}

	private String getFileName() {
		return "planner"+fileCount++ +".tg";
	}

	/**
	 * Save the original planner's graph g
	 * @param originalPlanner
	 * @param fileName
	 */
	private void saveGraph(PKSLPlanner originalPlanner, String fileName) {
		Builder<GryoIo> builder = IoCore.gryo();
		builder.graph(originalPlanner.g);
		IoRegistry kryo = new MyGraphIoRegistry();
		builder.registry(kryo);
		GryoIo yes = builder.create();
		try {
			yes.writeGraph(fileName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Read the cached version of the planner 
	 * into a new planner for use within a scenario
	 * @return
	 */
	private PKSLPlanner getNewPlannerCopy() {
		PKSLPlanner newPlanner = new PKSLPlanner();
		// using the flushed out original planner
		// read it back into a new graph
		Builder<GryoIo> builder = IoCore.gryo();
		builder.graph(newPlanner.g);
		IoRegistry kryo = new MyGraphIoRegistry();
		builder.registry(kryo);
		GryoIo yes = builder.create();
		try {
			yes.readGraph(this.fileName);
		} catch (IOException e) {
			e.printStackTrace();
		}

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

	private Map<String, PKSLPlanner> getScenarioMap(Iterator<IHeadersDataRow> iterator) {
		// key is scenario, value is the map store for that scenario
		Map<String, PKSLPlanner> plannerMap = new HashMap<>();
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
			PKSLPlanner scenarioPlanner = null;
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

	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
}
