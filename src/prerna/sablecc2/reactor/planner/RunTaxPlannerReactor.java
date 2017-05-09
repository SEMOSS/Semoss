package prerna.sablecc2.reactor.planner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
import prerna.sablecc2.reactor.TablePKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;
import prerna.util.ArrayUtilityMethods;

public class RunTaxPlannerReactor extends AbstractTablePlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());
	private static int fileCount = 0;
	String scenarioHeader = "Proposal"; //header of column containing Trump, House, etc
	String aliasHeader = "Alias"; //header for value containing our column name
	String valueHeader = "Value"; //header for value containing the value assigned to column name
	
	
	String OPERATION_COLUMN = "OP";
	String NOUN_COLUMN = "NOUN";
	String DIRECTION_COLUMN = "DIRECTION";
	
	String inDirection = "IN";
	String outDirection = "OUT";
	
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
		
		List<String> pksls = getPksls(); 
		Map<String, InMemStore> mapStore = getMemStores(getIterator());
		InMemStore returnStore = new MapStore();
		
		for(String scenario : mapStore.keySet()) {
			
			//grab the scenario and set the vars to the planner before excecuting
			InMemStore nextScenario = mapStore.get(scenario);
			
			//should i use a new translation every time or the same one?
			setVarsToPlanner(translation.planner, nextScenario);
			
			for(String pkslString : pksls) {
				try {
					Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
					Start tree = p.parse();
					tree.apply(translation);
				} catch (ParserException | LexerException | IOException e) {
					e.printStackTrace();
				} catch(Exception e) {
					e.printStackTrace();
				}
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
	
	private List<String> getPksls() {
		TablePKSLPlanner planner = getPlanner();
		List<String> pksls = collectNextPksls(planner);
		return pksls;
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
	
	private Iterator<IHeadersDataRow> getIterator() {
		GenRowStruct allNouns = getNounStore().getNoun("SCENARIOS");
		Iterator iterator = null;
		
		if(allNouns != null) {
			Job job = (Job)allNouns.get(0);
			iterator = job.getIterator();
		}
		return iterator;
	}
	
	private Map<String, InMemStore> getMemStores(Iterator<IHeadersDataRow> iterator) {
		
		//key is scenario, value is the map store for that scenario
		Map<String, InMemStore> memStoreMap = new HashMap<>();
		
		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			
			//TODO: move this outside so we don't calculate every time
			String[] headers = nextData.getHeaders();
			int scenarioHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, scenarioHeader);
			int aliasHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
			int valueHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, valueHeader);
			
			//grab each row
			Object[] values = nextData.getValues();
			
			//identify which scenario this is
			String scenario = values[scenarioHeaderIndex].toString();
			
			//grab alias and value (value should be literal, number, or column?)
			String alias = values[aliasHeaderIndex].toString();
			Object value = values[valueHeaderIndex];
			
			//add to its specific scenario map store
			if(memStoreMap.containsKey(scenario)) {
				memStoreMap.get(scenario).put(alias, PkslUtility.getNoun(value));
			} else {
				MapStore newMap = new MapStore();
				newMap.put(alias, null);
				memStoreMap.put(scenario, newMap);
			}
			
		}
		return memStoreMap;
	}	
}
