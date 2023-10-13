package prerna.reactor.planner.graph;
//package prerna.sablecc2.reactor.planner.graph;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
//import org.apache.tinkerpop.gremlin.structure.io.IoCore;
//import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
//import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
//
//import prerna.engine.api.IHeadersDataRow;
//import prerna.sablecc2.GreedyTranslation;
//import prerna.sablecc2.LazyTranslation;
//import prerna.sablecc2.PkslUtility;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.InMemStore;
//import prerna.sablecc2.om.Job;
//import prerna.sablecc2.om.NounMetadata;
//import prerna.sablecc2.om.PkslDataTypes;
//import prerna.sablecc2.om.TaxMapStore;
//import prerna.sablecc2.reactor.BaseJavaRuntime;
//import prerna.sablecc2.reactor.PKSLPlanner;
//import prerna.util.ArrayUtilityMethods;
//import prerna.util.MyGraphIoRegistry;
//
//public class RunJavaGraphTaxPlannerReactor extends AbstractPlannerReactor {
//
//	private static final Logger LOGGER = LogManager.getLogger(RunJavaGraphTaxPlannerReactor.class.getName());
//	
//	private static int fileCount = 0;
//	
//	private static final String PROPOSAL_NOUN = "PROPOSALS";
//	private String scenarioHeader;
//	private String aliasHeader;
//	private String valueHeader;
//	private String typeHeader;
//
//	private PKSLPlanner originalPlan = null;
//	private String fileName;
//	
//	public RunJavaGraphTaxPlannerReactor() {
//		setDefaults();
//	}
//	
//	private void setDefaults() {
//		scenarioHeader = "ProposalName"; //header of column containing Trump, House, etc
////		aliasHeader = "Alias_1"; //header for value containing our column name
//		aliasHeader = "Hashcode"; //header for value containing our column name
//		valueHeader = "Value_1"; //header for value containing the value assigned to column name
//		typeHeader = "Type_1";
//	}
//	
//	@Override
//	public void In() {
//		curNoun("all");
//	}
//
//	@Override
//	public Object Out() {
//		return parentReactor;
//	}
//
//	@Override
//	public NounMetadata execute()
//	{
//		long start = System.currentTimeMillis();
//
//		// get the original version of the plan we want to save
//		this.originalPlan = getPlanner();
//		// remove this stupid thing!!!
//		this.originalPlan.g.traversal().V().has(PKSLPlanner.TINKER_ID, "OP:FRAME").drop().iterate();
//		// save the location
//		this.fileName = getFileName();
//		saveGraph(this.originalPlan, this.fileName);
//
//		// now loop through and store all the necessary information
//		// around each proposal
//		Iterator<IHeadersDataRow> scenarioIterator = getIterator();
//		// iterate through the scenario information
//		// and generate a pksl planner for each scenario
//		Map<String, PKSLPlanner> scenarioMap = getScenarioMap(scenarioIterator);
//		
//		// create the master return store
//		// this will contain each scenario
//		// pointing to another map
//		// for that maps specific information
//		InMemStore returnStore = new TaxMapStore();
//
//		List<PKSLPlanner> planners = new ArrayList<>();
//		for(String scenario : scenarioMap.keySet()) {
//			LOGGER.info("Start execution for scenario = " + scenario);
//
//			// create a new translation to run through
//			GreedyTranslation translation = new GreedyTranslation();
//			// get the planner for the scenario
//			PKSLPlanner nextScenario = scenarioMap.get(scenario);
//			nextScenario.addVariable("$SCENARIO", new NounMetadata(scenario, PkslDataTypes.CONST_STRING));
//			translation.planner = nextScenario;
//			
//			// iterate through to determine execution order for
//			// the scenario
//			List<String> pkslList = getPksls(nextScenario);
//			
//			Map<String, String> mainMap = (Map)nextScenario.getProperty("MAIN_MAP", "MAIN_MAP");
////			for(String key : mainMap.keySet()) {
////				System.out.println(key+":::"+mainMap.get(key));
////			}
//			
//			List<String> fieldsList = buildFields(mainMap, nextScenario);
////			List<String> newPksls = buildPksl(pkslList, mainMap);
////			for(String pksl : newPksls) {
////				System.out.println(pksl);
////			}
//			
//			System.out.println("Total"+pkslList.size());
//			
//			long startTime = System.currentTimeMillis();
//			RuntimeJavaClassBuilder builder = new RuntimeJavaClassBuilder();
//			builder.addEquations(pkslList);
//			builder.addFields(fieldsList);
//			BaseJavaRuntime javaClass = builder.buildClass();
//			javaClass.execute();
//			long endTime = System.currentTimeMillis();
////			for(String equation : builder.equations) {
////				System.out.println(equation);
////			}
////			javaClass.execute();
//			Map<String, Object> results = javaClass.getVariables();
////			for(String key : results.keySet()) {
////				System.out.println(key+":::"+results.get(key));
////			}
////			System.out.println((endTime - startTime)+" ms");
//			//need to take the results from the java class and add to the translation's planner
//			
////			PkslUtility.addPkslToTranslation(translation, pkslList);
//			
//			LOGGER.info("End execution for scenario = " + scenario);
//			// after execution
//			// we need to	 store the information
//			LOGGER.info("Start storing data inside store");
//			
//			planners.add(translation.planner);
//			
//		}
//		
//		long end = System.currentTimeMillis();
//		System.out.println("****************    END RUN TAX PLANNER "+(end - start)+"ms      *************************");
//
//		File file = new File(this.fileName);
//		file.delete();
//		return new NounMetadata(planners, PkslDataTypes.PLANNER);
////		return new NounMetadata(returnStore, PkslDataTypes.IN_MEM_STORE);
//	}
//
//	private List<String> getPksls(PKSLPlanner planner) {
//		// keep track of all the pksls to execute
//		List<String> pksls = new Vector<String>();
//
//		// using the root vertices
//		// iterate down all the other vertices and add the signatures
//		// for the desired travels in the appropriate order
//		// note: this is adding to the list of undefined variables
//		// calculated at beginning of class 
//		traverseDownstreamVertsProcessor(planner, pksls);
//		return pksls;
//	}
//
//	private String getFileName() {
//		return "planner"+fileCount++ +".gio";
//	}
//
//	/**
//	 * Save the original planner's graph g
//	 * @param originalPlanner
//	 * @param fileName
//	 */
//	private void saveGraph(PKSLPlanner originalPlanner, String fileName) {
//		// try the default
//		long start = System.currentTimeMillis();
//		Builder<GryoIo> builder = IoCore.gryo();
//		builder.graph(originalPlanner.g);
//		IoRegistry kryo =  new MyGraphIoRegistry();
//		builder.registry(kryo);
//		GryoIo yes = builder.create();
//		try {
//			yes.writeGraph(fileName);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} 
//		
//		long end = System.currentTimeMillis();
//		System.out.println("FINISHED WRITING GRAPH : " + (end-start));
//	}
//
//	private void saveGraph2(PKSLPlanner originalPlanner, String fileName) {
//		long curTime = System.currentTimeMillis();
//		try {
//			originalPlanner.g.io(IoCore.graphson()).writeGraph(fileName);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			long endTime = System.currentTimeMillis();
//			System.out.println("FINISHED WRITING GRAPH: "+(endTime - curTime)+" ms");
//		}
//	}
//	
//	/**
//	 * Read the cached version of the planner 
//	 * into a new planner for use within a scenario
//	 * @return
//	 */
//	private PKSLPlanner getNewPlannerCopy() {
//		long start = System.currentTimeMillis();
//
//		PKSLPlanner newPlanner = new PKSLPlanner();
//		// using the flushed out original planner
//		// read it back into a new graph
//		long curTime = System.currentTimeMillis();
//		Builder<GryoIo> builder = IoCore.gryo();
//		builder.graph(newPlanner.g);
//		IoRegistry kryo =  new MyGraphIoRegistry();
//		builder.registry(kryo);
//		GryoIo yes = builder.create();
//		try {
//			yes.readGraph(this.fileName);
//			long endTime = System.currentTimeMillis();
//			System.out.println("FINISHED READING GRAPH: "+(curTime - endTime)+" ms");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		long end = System.currentTimeMillis();
//		System.out.println("FINISHED READING GRAPH : " + (end-start));
//		
//		newPlanner.g.createIndex(PKSLPlanner.TINKER_TYPE, Vertex.class);
//		newPlanner.g.createIndex(PKSLPlanner.TINKER_ID, Vertex.class);
//
//		// now loop through the original planner
//		// and set all the variables that are defined
//		// into the new planner
//		Set<String> variables = this.originalPlan.getVariables();
//		for(String varName : variables) {
//			NounMetadata varNoun = this.originalPlan.getVariable(varName);
//			if(varNoun.isScalar()) {
////				System.out.println("Orig values ::: " + varName + " > " + this.originalPlan.getVariable(varName));
//				newPlanner.addVariable(varName, varNoun);
//			}
//		}
//		
//		return newPlanner;
//	}
//
//	private Map<String, PKSLPlanner> getScenarioMap(Iterator<IHeadersDataRow> iterator) {
//		// key is scenario, value is the map store for that scenario
//		Map<String, PKSLPlanner> plannerMap = new HashMap<>();
//		// define a central translation
//		// to execute everything with
//		// but substituting with the correct scenario planner
//		LazyTranslation translation = new LazyTranslation();
//		while(iterator.hasNext()) {
//			IHeadersDataRow nextData = iterator.next();
//
//			//TODO: move this outside so we don't calculate every time
//			String[] headers = nextData.getHeaders();
//			int scenarioHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, scenarioHeader);
//			int aliasHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
//			int valueHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, valueHeader);
//			int typeHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, typeHeader);
//
//			//grab each row
//			Object[] values = nextData.getValues();
//
//			//identify which scenario this is
//			String scenario = values[scenarioHeaderIndex].toString();
//
//			//grab alias and value (value should be literal, number, or column?)
//			String alias = values[aliasHeaderIndex].toString();
//			Object value = values[valueHeaderIndex];
//
//			String type = values[typeHeaderIndex].toString();
//			boolean isFormula = "formula".equalsIgnoreCase(type);
//
//			//add to its specific scenario map store
//			PKSLPlanner scenarioPlanner = null;
//			if(plannerMap.containsKey(scenario)) {
//				scenarioPlanner = plannerMap.get(scenario);
//			} else {
//				scenarioPlanner = getNewPlannerCopy();
//				plannerMap.put(scenario, scenarioPlanner);
//			}
//			translation.planner = scenarioPlanner;
//			// if it is a formula
//			// parse and add to the scenario plan
//			// else, add it as a variable
//			if(isFormula) {
//				String pkslString = PkslUtility.generatePKSLString(alias, value);
//				PkslUtility.addPkslToTranslation(translation, pkslString);
////				System.out.println(pkslString);
//			} else {
//				scenarioPlanner.addVariable(alias, PkslUtility.getNoun(value));
////				System.out.println(alias+" = "+value);
//			}
//		}
//		return plannerMap;
//	}
//
//	@Override
//	protected String getPksl(Vertex vert) {
//		try {
//			if(vert.property("REACTOR_TYPE") != null) {
//				if(vert.property("JAVA_SIGNATURE") != null) {
//					String pkslOperation = vert.property("JAVA_SIGNATURE").value()+"";
//					if(pkslOperation.isEmpty()) {
//						return pkslOperation;
//					}
//					else return pkslOperation+";";
//				}
//			}
//		} catch(Exception e) {
//			return "";//super.getPksl(vert);
//		}
//		return "";
//	}
//	
//	private List<String> buildPksl(List<String> pkslOperations, Map<String, String> mainMap) {
//		List<String> newPksls = new ArrayList<>();
//		for(String pksl : pkslOperations) {
//			String assignment = pksl.split("=")[0].trim();
//			String value = mainMap.get(assignment);
//			while(value != null && (!value.equals("double") && !value.equals("boolean") && !value.equals("String") && !value.equals("int"))) {
//				value = mainMap.get(value);
//			}
//			
//			if(value == null) {
//				newPksls.add("Object "+pksl);
//			} else {
//				newPksls.add(value+" "+pksl);
//				
//			}
//		}
//		return newPksls;
//	}
//	
//	private List<String> buildFields(Map<String, String> mainMap, PKSLPlanner planner) {
//		List<String> fields = new ArrayList<>();
//		Set<String> assignedFields = new HashSet<>();
//		
//		for(String assignment : mainMap.keySet()) {
//			String value = mainMap.get(assignment);
//			
//			boolean isNumber = isNumber(value);
//			
//			while(!isNumber && value != null && (!value.equals("double") && !value.equals("boolean") && !value.equals("String") && !value.equals("int"))) {
//				value = mainMap.get(value);
//			}
//			
//			if(value == null) {
//				NounMetadata noun = planner.getVariableValue(assignment);
//				if(noun != null) {
//					PkslDataTypes nounType = noun.getNounName();
//					String field = "";
//					Object nounValue = noun.getValue();
//					if(nounType == PkslDataTypes.CONST_DECIMAL || nounType == PkslDataTypes.CONST_INT) {
//						field = "double "+assignment+" = "+nounValue+";";
//					} else if(nounType == PkslDataTypes.CONST_STRING) {
//						field = "String "+assignment+" = \""+nounValue+"\";";
//					} else if(nounType == PkslDataTypes.BOOLEAN) {
//						field = "boolean "+assignment+" = "+nounValue+";";
//					}
//					
//					if(!assignedFields.contains(assignment)) {
//						fields.add(field);
//						assignedFields.add(assignment);
//					}
//					
//				} else {
//					
//				}
//			} else if(isNumber) { 
//				String field = "public double "+" "+assignment + " = "+value+";";
//				if(!assignedFields.contains(assignment)) {
//					fields.add(field);
//					assignedFields.add(assignment);
//				}
//			} else {
//				String field = "public "+value+" "+assignment;
//				if(value.equals("double") || value.equals("int")) {
//					field += " = 0.0;";
//				} else if(value.equals("String")) {
//					field += " = \"\";";
//				} else if(value.equals("boolean")) {
//					field += " = true;";
//				} 
//				
//				if(!assignedFields.contains(assignment)) {
//					fields.add(field);
//					assignedFields.add(assignment);
//				}
//			}
//		}
//		
//		for(String assignment : planner.getVariables()) {
//			NounMetadata noun = planner.getVariableValue(assignment);
//			if(noun != null) {
//				PkslDataTypes nounType = noun.getNounName();
//				Object nounValue = noun.getValue();
//				String field = "";
//				if(nounType == PkslDataTypes.CONST_DECIMAL || nounType == PkslDataTypes.CONST_INT) {
//					field = "double "+assignment+" = "+nounValue+";";
//				} else if(nounType == PkslDataTypes.CONST_STRING) {
//					field = "String "+assignment+" = \""+nounValue+"\";";
//				} else if(nounType == PkslDataTypes.BOOLEAN) {
//					field = "boolean "+assignment+" = "+nounValue+";";
//				}
//				
//				if(!assignedFields.contains(assignment)) {
//					fields.add(field);
//					assignedFields.add(assignment);
//				}
//			} else {
//				
//			}
//		}
//		
////		for(String field : fields) {
////			System.out.println(field);
////		}
//		return fields;
//	}
//	
//	private boolean isNumber(String value) {
//		try {
//			double doub = Double.parseDouble(value);
//			return true;
//		} catch(Exception e) {
//			return false;
//		}
//	}
//	/****************************************************
//	 * METHODS TO GRAB VALUES FROM REACTOR
//	 ***************************************************/
//
//	private Iterator<IHeadersDataRow> getIterator() {
//		GenRowStruct allNouns = getNounStore().getNoun("PROPOSALS");
//		Iterator<IHeadersDataRow> iterator = null;
//
//		if(allNouns != null) {
//			Job job = (Job)allNouns.get(0);
//			iterator = job.getIterator();
//		}
//		return iterator;
//	}
//
//	private PKSLPlanner getPlanner() {
//		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
//		PKSLPlanner planner = null;
//		if(allNouns != null) {
//			planner = (PKSLPlanner) allNouns.get(0);
//			return planner;
//		} else {
//			return this.planner;
//		}
//	}
//}
