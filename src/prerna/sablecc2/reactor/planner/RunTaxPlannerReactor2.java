package prerna.sablecc2.reactor.planner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;
import prerna.util.ArrayUtilityMethods;
import prerna.util.MyGraphIoRegistry;

public class RunTaxPlannerReactor2 extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());
	private static int fileCount = 0;
	String scenarioHeader = "ProposalName"; //header of column containing Trump, House, etc
	String aliasHeader = "Alias_1"; //header for value containing our column name
	String valueHeader = "Value_1"; //header for value containing the value assigned to column name
	String typeHeader = "Type_1";
	String fileName = getFileName();
	
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
		
		Translation translation = new Translation();
		
		//save a copy of the original planner we want to deserialize for each plan
		PKSLPlanner planner = getPlanner();
		saveGraph(planner);
		
			
		Map<String, PKSLPlanner> mapStore = getPlanners(getIterator());
		InMemStore returnStore = new MapStore();
		
		for(String scenario : mapStore.keySet()) {
			
			//grab the scenario and set the vars to the planner before excecuting
			PKSLPlanner nextScenario = mapStore.get(scenario);
			
			translation.planner = nextScenario;
			
			for(String pkslString : getPksls(nextScenario)) {
				try {
					Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
					Start tree = p.parse();
					tree.apply(translation);
				} catch (ParserException | LexerException | IOException e) {
					e.printStackTrace();
				} catch(Exception e) {
					e.printStackTrace();
				}
				System.out.println(pkslString);
			}
		

			InMemStore resultScenarioStore = new MapStore();
			Set<String> variables = translation.planner.getVariables();
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
		}
		
		long end = System.currentTimeMillis();
		System.out.println("****************    "+(end - start)+"      *************************");
		
		return new NounMetadata(returnStore, PkslDataTypes.IN_MEM_STORE);
	}
	
	private void setVarsToPlanner(PKSLPlanner planner, InMemStore memStore) {
		Set<Object> keys = memStore.getStoredKeys();
		for(Object key : keys) {
			planner.addVariable(key.toString(), memStore.get(key));
		}
	}
	
	private List<String> getPksls(PKSLPlanner planner) {
		// we use this just for debugging
		// this will get undefined variables and set them to 0
		// ideally, we should never have to do this...
		List<String> pksls = getUndefinedVariablesPksls(planner);
		
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
	
	private void saveGraph(PKSLPlanner planner) {
		
		
		Iterator<Vertex> it = planner.g.vertices();
		while(it.hasNext()) {
			Vertex v = it.next();
			
			Iterator<VertexProperty<Object>> props = v.properties();
			while(props.hasNext()) {
				VertexProperty<Object> property = props.next();
				System.out.println(property.key());
				System.out.println(property.value());
			}
		}
		
		Iterator<Edge> it2 = planner.g.edges();
		while(it2.hasNext()) {
			Edge v = it2.next();
			
			Iterator<Property<Object>> props = v.properties();
			while(props.hasNext()) {
				Property<Object> property = props.next();
				System.out.println(property.key());
				System.out.println(property.value());
			}
		}
		
		Builder<GryoIo> builder = IoCore.gryo();
		builder.graph(planner.g);
		IoRegistry kryo = new MyGraphIoRegistry();
		builder.registry(kryo);
		GryoIo yes = builder.create();
		try {
			yes.writeGraph(fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private PKSLPlanner getPlannerCopy() {
		PKSLPlanner origPlanner = getPlanner();
		PKSLPlanner planner = new PKSLPlanner();
		
		//set the variables
		for(String varName : origPlanner.getVariables()) {
			origPlanner.addVariable(varName, origPlanner.getVariable(varName));
		}
		
		Builder<GryoIo> builder = IoCore.gryo();
		builder.graph(planner.g);
		IoRegistry kryo = new MyGraphIoRegistry();
		builder.registry(kryo);
		GryoIo yes = builder.create();
		try {
			yes.readGraph(fileName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return planner;
	}
	
	private Map<String, PKSLPlanner> getPlanners(Iterator<IHeadersDataRow> iterator) {
		
		//key is scenario, value is the map store for that scenario
		Map<String, PKSLPlanner> plannerMap = new HashMap<>();
		
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
			if(plannerMap.containsKey(scenario)) {
				if(isFormula) {
					String pkslString = PkslUtility.generatePKSLString(alias, value);
					PkslUtility.addPkslToPlanner(plannerMap.get(scenario), pkslString);
				} else {
					plannerMap.get(scenario).addVariable(alias, PkslUtility.getNoun(value));
				}
			} else {
				PKSLPlanner newPlanner = getPlannerCopy();
				if(isFormula) {
					String pkslString = PkslUtility.generatePKSLString(alias, value);
					PkslUtility.addPkslToPlanner(newPlanner, pkslString);
				} else {
					newPlanner.addVariable(alias, PkslUtility.getNoun(value));
				}
				plannerMap.put(scenario, newPlanner);
			}
		}
		return plannerMap;
	}
	
	/****************************************************
	 * METHODS TO GRAB VALUES FROM REACTOR
	 ***************************************************/
	
	private Iterator<IHeadersDataRow> getIterator() {
		GenRowStruct allNouns = getNounStore().getNoun("PROPOSALS");
		Iterator iterator = null;
		
		if(allNouns != null) {
			Job job = (Job)allNouns.get(0);
			iterator = job.getIterator();
		}
		return iterator;
	}
	
	private InMemStore getInMemoryStore() {
		InMemStore inMemStore = null;
		GenRowStruct grs = getNounStore().getNoun(PkslDataTypes.IN_MEM_STORE.toString());
		if(grs != null) {
			inMemStore = (InMemStore) grs.get(0);
		}
		
		if(inMemStore == null) {
			return new MapStore();
		}
		return inMemStore;
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
