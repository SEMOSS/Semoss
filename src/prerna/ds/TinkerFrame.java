package prerna.ds;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.om.TinkerGraphDataModel;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.GremlinBuilder;
import prerna.sablecc.Translation;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class TinkerFrame implements ITableDataFrame {
	
	private static final Logger LOGGER = LogManager.getLogger(TinkerFrame.class.getName());
	
	//Column Names of the table
	protected String[] headerNames = null;
	protected List<String> columnsToSkip = new Vector<String>(); //make a set?
//	protected List<String> selectors = new Vector<String>();
	
	//keeps the cache of whether a column is numerical or not, can this be stored on the meta model?
	protected Map<String, Boolean> isNumericalMap = new HashMap<String, Boolean>(); //store on meta
			
	protected List<Object> algorithmOutput = new Vector<Object>();
	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected TinkerGraph g = null;
	
	int startRange = -1;
	int endRange = -1;

	final protected static String PRIM_KEY = "PRIM_KEY";
	final public static String META = "META";
	final public static String EMPTY = "_";
	
	private Object tempExpressionResult = "FAIL";

	/**********************    TESTING PLAYGROUND  ******************************************/
	
	public static void main(String [] args) throws Exception
	{
		
		TinkerFrame t3 = new TinkerFrame();
		testPaths();
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
//		TinkerFrame t = load2Graph4Testing(fileName);
//		t.openSandbox();
//		testGroupBy();
//		testFilter();
//		testCleanup();
//		new TinkerFrame().doTest();
		//t3.writeToFile();
		//t3.readFromFile();
		//t3.doTest();
//		t3.tryCustomGraph();
//		t3.tryFraph();
		/*
		Configuration config = new BaseConfiguration();
		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
		
		
		Graph g = TinkerGraph.open(); TinkerFactory.createModern();
		
		Vertex b = g.addVertex("y");
		b.property("name", "y");
		for(int i = 0;i < 1000000;i++)
		{
			Vertex a = g.addVertex("v" + i);
			a.property("name", "v"+i);
			b.addEdge("generic", a);
			//Edge e = g.(null, a, b, "sample");
		}	
		System.out.println("here.. ");
		Vertex v = g.traversal().V().has("name", "v1").next();
		//Vertex v = g.("name", "y").iterator().next();
		System.out.println("done.. ");
		
		
		g.close();
		
		
		
		g = TinkerFactory.createModern();
		
		
		// experiments
		*/
		
		
		
		
	}

	public static void testGroupBy() {
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
		TinkerFrame tinker = load2Graph4Testing(fileName);
		
		TinkerFrameStatRoutine tfsr = new TinkerFrameStatRoutine();
		Map<String, Object> functionMap = new HashMap<String, Object>();
		functionMap.put("math", "count");
		functionMap.put("name", "Studio");
		functionMap.put("calcName", "NewCol");
		functionMap.put("GroupBy", new String[]{"Studio"});
		
		tfsr.setSelectedOptions(functionMap);
		tfsr.runAlgorithm(tinker);
	}
	
	public static void testPaths() {
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
		TinkerFrame tinker = load2Graph4Testing(fileName);
		tinker.printTinker();
		
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(Arrays.asList(tinker.getColumnHeaders()), tinker.g);
		GraphTraversal paths = builder.executeScript().path();
//		Object o = paths.next();
		int count = 0;
		while(paths.hasNext()){
		System.out.println(paths.next());
		count++;
		}
		System.out.println(count);
	}
	
	public static void testFilter() {
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
		TinkerFrame tinker = load2Graph4Testing(fileName);
		tinker.printTinker();
		
		
		Object[] uniqValues = tinker.getUniqueValues("Studio");
		List<Object> filterValues = new ArrayList<Object>();
		for(Object o : uniqValues) {
			if(!(o.toString().equals("CBS"))) {
				filterValues.add(o);
			}
		}
		tinker.filter("Studio", filterValues);		
		tinker.printTinker();
		
		
		uniqValues = tinker.getUniqueValues("Genre Updated");
		filterValues = new ArrayList<Object>();
		filterValues.add("Drama");
		tinker.filter("Genre Updated", filterValues);
		tinker.printTinker();
		//print tinker
		
//		tinker.unfilter("Title");
		
		//print tinker
		
		tinker.unfilter();
		tinker.printTinker();
		//print tinker
	}
	
	public static void testCleanup() {
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
		TinkerFrame tinker = load2Graph4Testing(fileName);
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(tinker.getSelectors(), tinker.g);
		GraphTraversal traversal = (GraphTraversal)builder.executeScript();
		traversal = traversal.V();
		GraphTraversal traversal2 = tinker.g.traversal().V();
		Set<Vertex> deleteVertices = new HashSet<Vertex>();
		while(traversal2.hasNext()) {
			deleteVertices.add((Vertex)traversal2.next());
		}
		while(traversal.hasNext()) {
			deleteVertices.remove((Vertex)traversal.next());
		}
		
		for(Vertex v : deleteVertices) {
			v.remove();
		}
//		System.out.println(t.size());
//		for(Object key : t.keySet()) {
//			Object o = t.get(key);
//			System.out.println(key instanceof Vertex);
//			System.out.println(o.toString());
//		}
		System.out.println("Done");
	}

	private void printTinker() {
		Iterator<Object[]> iterator = this.iterator(false);
		while(iterator.hasNext()) {
			System.out.println(Arrays.toString(iterator.next()));
		}
	}
	
	public void tryCustomGraph()
	{
		g = TinkerGraph.open();
		
		long now = System.nanoTime();
		System.out.println("Time now.. " + now);
		
		String [] types = {"Capability", "Business Process", "Activity", "DO", "System"};
//		String[] types = {"Capability"};
		int [] nums = new int[5];
//		int [] nums = new int[1];
		nums[0] = 2;
		nums[1] = 5;
		nums[2] = 8;
		nums[3] = 1;
		nums[4] = 1;
		
		for(int typeIndex = 0;typeIndex < types.length;typeIndex++)
		{
			String parentTypeName = types[typeIndex];
			if(typeIndex + 1 < types.length)
			{
				String childTypeName = types[typeIndex + 1];
				int numParent = nums[typeIndex];
				int numChild = nums[typeIndex+1];
			
				for(int parIndex = 0;parIndex < numParent;parIndex++)
				{
					Vertex parVertex = upsertVertex(parentTypeName, parentTypeName + parIndex, parentTypeName + parIndex);
					parVertex.property("DATA", parIndex);
					for(int childIndex = 0;childIndex < numChild;childIndex++)
					{
						Vertex childVertex = upsertVertex(childTypeName, childTypeName + childIndex, childTypeName + childIndex);
						Object data = childIndex;
						childVertex.property("DATA", data);
						upsertEdge(parVertex, childVertex);	
					}
				}
				Map <String, Set<String>> edges = new Hashtable <String, Set<String>>();
				Set set = new HashSet();
				set.add(childTypeName);
				edges.put(parentTypeName, set);
				mergeEdgeHash(edges);
			}
			
			else {
				//just add a vertex
				Vertex vert = upsertVertex(types[0], types[0]+"1", types[0]+"1");
				Vertex vert2 = upsertVertex(types[0], types[0]+"2", types[0]+"2");
			}
		}
		
		long graphTime = System.nanoTime();
		System.out.println("Time taken.. " + ((graphTime - now) / 1000000000) + " secs");

		
		System.out.println("Graph Complete.. ");
		
		System.out.println("Total Number of vertices... ");
		GraphTraversal gtCount = g.traversal().V().count();
		if(gtCount.hasNext())
			System.out.println("Vertices...  " + gtCount.next());

		GraphTraversal gtECount = g.traversal().E().values("COUNT").sum();
		if(gtECount.hasNext())
			System.out.println("Edges...  " + gtECount.next());

		
		System.out.println("Trying group by on the custom graph");
		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by("TYPE").by(__.count());
//		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by("TYPE").by(__.);
		if(gt.hasNext())
			System.out.println(gt.next());
		System.out.println("Completed group by");
		
		System.out.println("Trying max");
		GraphTraversal<Vertex, Number> gt2 = g.traversal().V().has("TYPE", "Activity").values(Constants.NAME).max();
		if(gt2.hasNext())
			System.out.println(gt2.next());
		System.out.println("Trying max - complete");

		System.out.println("Trying max group");
		GraphTraversal  gt3 = g.traversal().V().group().by("TYPE").by(__.values("DATA").max());
		if(gt3.hasNext())
			System.out.println(gt3.next());
		System.out.println("Trying max group - complete");
		
		
		GraphTraversal<Vertex, Map<String, Object>> gtAll = g.traversal().V().has("TYPE", "Capability").as("Cap").
				out().V().as("BP").
				out().as("Activity").
				out().as("DO").
				out().as("System").
				range(4, 10).
				select("Cap", "BP", "Activity", "DO", "System").by("VALUE");
		int count = 0;
		while(gtAll.hasNext())
		{
			//Map output = gtAll.next();
			System.out.println(gtAll.next());
			count++;
		}
		System.out.println("Count....  " + count);
		
		/*
		GraphTraversal<Vertex, Long> gtAllC = g.traversal().V().has("TYPE", "Capability").as("Cap").out().as("BP").
				out().as("Activity").
				out().as("DO").out().as("System").
				select("Cap", "BP", "Activity", "DO", "System").count();
		
		while(gtAllC.hasNext())
		{
			System.out.println("Total..  " + gtAllC.next());
		}*/


		long opTime = System.nanoTime();
		System.out.println("Time taken.. for ops" + ((opTime - graphTime) / 1000000000) + " secs");
		
		headerNames = types;

		Vector <String> cols = new Vector<String>();
		cols.add("Capability"); 
		cols.add("Business Process");
		//, "Activity", "DO", "System"};

		//getRawData();
		Iterator out = getIterator(cols);
		if(out.hasNext())
			System.out.println("Output is..  " + out.next());
	}

	private void tryBuilder()
	{
		
		
		
	}
	
	public void openSandbox() {
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine();				
			}
		};
	
		thread.start();
	}
	
	public void writeToFile()
	{
		Configuration config = new BaseConfiguration();
		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
		
		
		Graph g = TinkerGraph.open(config); 
		
		Vertex b = g.addVertex("y");
		b.property("name", "y");
		for(int i = 0;i < 1000000;i++)
		{
			Vertex a = g.addVertex("v" + i);
			a.property("name", "v"+i);
			b.addEdge("generic", a);
			//Edge e = g.(null, a, b, "sample");
		}	
		System.out.println("here.. ");
		Vertex v = g.traversal().V().has("name", "v1").next();
		//Vertex v = g.("name", "y").iterator().next();
		System.out.println("done.. ");
		
		
		try {
			System.out.println("Writing to file... ");
			g.close();
			System.out.println("written");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void readFromFile(){
		Configuration config = new BaseConfiguration();
		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
		
		
		Graph g = TinkerGraph.open(); 
		try {
			long time = System.nanoTime();

			System.out.println("reading from file... ");

			g.io(IoCore.gryo()).readGraph("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
			long delta = System.nanoTime() - time;
			System.out.println("Search time in nanos " + (delta/1000000000));
			
			System.out.println("complte");
			Vertex v = g.traversal().V().has("name", "v1").next();
			//Vertex v = g.("name", "y").iterator().next();
			System.out.println("done.. " + v);
			
			
			g.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void testCount() {
		GraphTraversal<Vertex, Map<String, Object>> gtAll = g.traversal().V().has("TYPE", "Capability").as("Cap").
				out().V().as("BP").
				out().as("Activity").
				out().as("DO").
				out().as("System").
				range(4, 10).
				select("Cap", "BP", "Activity", "DO", "System").by("VALUE");
		
//		g.traversal().V().ou
	}
	
	public void doTest()
	{
		Graph g = TinkerFactory.createModern();
		
		// trying to see the path
		GraphTraversal  <Vertex,Path> gt = g.traversal().V().as("a").out().as("b").out().values("name").path(); //.select("a","b");
		while(gt.hasNext())
		{
			Path thisPath = gt.next();
			for(int index = 0;index < thisPath.size();index++)
			{
				//Vertex v = (Vertex)thisPath.get()
				System.out.print(thisPath.get(index) + "");
			}
			System.out.println("\n--");
		}
		

		System.out.println("Trying select.. ");
		
		
		GraphTraversal<Vertex, Map<String, Object>> gt2 = g.traversal().V().as("a").out().as("b").out().as("c").select("a","b","c");
		
		while(gt2.hasNext())
		{
			Map<String, Object> map = gt2.next();
			Iterator <String> keys = map.keySet().iterator();
			while(keys.hasNext())
			{
				Vertex v = (Vertex)map.get(keys.next());
				System.out.print(v.value("name") + "-");
			}
			System.out.println("\n--");
		}

		System.out.println("Trying Group By.. ");
		
		GraphTraversal<Vertex,Map<Object,Object>> gt3 = g.traversal().V().as("a").group().by("name").by(__.count());
		
		while(gt3.hasNext())
		{
			Map<Object, Object> map = gt3.next();
			Iterator <Object> keys = map.keySet().iterator();
			while(keys.hasNext())
			{
				Object key = keys.next();
				System.out.print(key + "<>" + map.get(key));
			}
			System.out.println("\n--");
		}
		
		System.out.println("Trying coalesce.. ");
		
		GraphTraversal<Vertex, Object> gt4 = g.traversal().V().coalesce(__.values("lang"), __.values("name"));
		while(gt4.hasNext())
		{
			System.out.println(gt4.next());
			/*
			Map<Object, Object> map = gt4.next();
			Iterator <Object> keys = map.keySet().iterator();
			while(keys.hasNext())
			{
				Object key = keys.next();
				System.out.print(key + "<>" + map.get(key));
			}
			System.out.println("\n--");*/
		}

		System.out.println("Trying choose.. with constant");
		GraphTraversal<Vertex, Map<Object, Object>> gt5 = g.traversal().V().choose(__.has("lang"),__.values("lang"), __.constant("c#")).as("lang").group();
		while(gt5.hasNext())
		{
			System.out.println(gt5.next());
		}

		System.out.println("Trying choose.. with vertex");
		GraphTraversal<Vertex, Map<Object, Object>> gt6 = g.traversal().V().choose(__.has("lang"),__.as("a"), __.as("b")).group();
		while(gt6.hasNext())
		{
			System.out.println(gt6.next());
		}
		
		System.out.println("testing repeat.. ");
		GraphTraversal<Vertex, Path> gt7 = g.traversal().V(1).repeat(__.out()).times(2).path().by("name");
		while(gt7.hasNext())
		{
			Path thisPath = gt7.next();
			for(int index = 0;index < thisPath.size();index++)
			{
				//Vertex v = (Vertex)thisPath.get()
				System.out.print(thisPath.get(index) + "");
			}
			System.out.println("\n--");
		}

		System.out.println("Trying.. until.. ");
		//GraphTraversal<Vertex, Path> gt8 = g.traversal().V().as("a").until(__.has("name", "ripple")).as("b").repeat(__.out()).path().by("name");
		GraphTraversal<Vertex, Map<String, Object>> gt8 = g.traversal().V().as("a").where(__.has("name", "marko")).until(__.has("name", "ripple")).as("b").repeat(__.out()).select("a","b");
		while(gt8.hasNext())
		{
			Map thisPath = gt8.next();
			System.out.println(thisPath);
			/*for(int index = 0;index < thisPath.size();index++)
			{
				//Vertex v = (Vertex)thisPath.get()
				System.out.println(thisPath);
				System.out.print(thisPath.get(index));
			}*/
			System.out.println("\n--");
		}

		System.out.println("Trying arbitrary selects.. ");
		GraphTraversal<Vertex, Vertex> gt9 = g.traversal().V().as("a").out().as("b").out().as("c");
		GraphTraversal<Vertex, Vertex> gt10 = gt9.select("c");
		while(gt10.hasNext())
		{
			System.out.println(gt10.next());
			System.out.println("\n--");
		}

		//GroovyShell shell = new GroovyShell();
		
		System.out.println("Testing subgraph.. ");
		Graph sg = (Graph)g.traversal().E().hasLabel("knows").subgraph("subGraph").cap("subGraph").next();
		org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource source =  sg.traversal(sg.traversal().standard());
		GraphTraversal <Edge, Edge> output = source.E();
	
		while(output.hasNext())
		{
			System.out.println(output.next().inVertex().id());
		}
		System.out.println("It is a subgraph now.. ");
		
		System.out.println("Testing partition"); // useful when I want to roll back and move forward // can also be used for filtering..
		SubgraphStrategy stratA = SubgraphStrategy.build().vertexCriterion(__.hasId(1)).create(); // putting id 1 into a separate subgraph
		//GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", P.not(P.within("marko", "ripple")));
		String [] names = {"marko", "ripple"};
		GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", P.without(names));
		
		while(gt11.hasNext())
		{
			System.out.println(gt11.next());
		}
		System.out.println("Now printing from partition");
		GraphTraversalSource newGraph = GraphTraversalSource.build().with(stratA).create(g);
		gt11 = newGraph.V().has("name", P.within("marko"));
		//gt11 = newGraph.V();
		
		while(gt11.hasNext())
		{
			System.out.println(gt11.next());
		}
		
		
		System.out.println("Testing.. variables.. ");
		
		String [] values = {"Hello World", "too"};
		g.variables().set("Data",values);
		
		System.out.println("Column Count ? ...  " + (
				(String[])(g.variables().get("Data").get())
				).length
				);
		
		System.out.println("Getting max values and min values.. ");
		System.out.println(g.traversal().V().values("age").max().next());
		System.out.println("Getting Sum.... ");
		System.out.println(g.traversal().V().values("age").sum().next());
		
		
		System.out.println("Trying count on vertex.. ");
		System.out.println(g.traversal().V().count().next());
		
		// Things to keep in the variables
		// String array of all the different nodes that are being added - Not sure given the graph
		// For every column - what is the base URI it came with, database which it came from, possibly host and server as well
		// For every column - also keep the logical name it is being referred to as vs. the logical name that it is being referred to in its native database
		// this helps in understanding how the user relates these things together
		// There is a default partition where the graph is added
		// Need to find how to adjust when a node gets taken out - use with a match may be ?
		//
		
		// counting number of occurences
		// I need to pick the metamodel partition
		// from this partition I need to find what are the connections
		// from one point to another
		
		

		// things I still need to try
		// 1. going from one node to a different arbitrary node
		// and then getting that subgraph
		
		// 2. Ability to find arbitrary set of nodes based on a filter on top of the 1
		
		
		
		
		
	}
	
	public void tryFraph()
    {
           Hashtable <String, Vector<String>> hash = new Hashtable<String, Vector<String>>();
           
           
           Vector <String> TV = new Vector<String>();
           TV.add("S");
           TV.add("G");
           
           Vector <String> SV = new Vector<String>();
           SV.add("A");
           SV.add("D");
           
           hash.put("T", TV);
           hash.put("S",SV);
           
           //Hashtable <String, Integer> outCounter = 
           
           // get the starting point
           String start = "T";
           String output = "";
           output = output + ".traversal().V()";
           Vector <String> nextRound = new Vector<String>();

           nextRound.add(start);
           
           boolean firstTime = true;
           
           while(nextRound.size() > 0)
           {
                  Vector <String> realNR = new Vector<String>();
                  System.out.println("Came in here.. ");
                  for(int nextIndex = 0;nextIndex < nextRound.size();nextIndex++)
                  {
                        String element = nextRound.remove(nextIndex);
                        if(hash.containsKey(element))
                        {
                               
                               output = output + ".has('" + "TYPE" + "','" + element + "')";
                               if(firstTime)
                               {
                                      output = output + ".as('" + element + "')";
                                      firstTime = false;
                               }
                               Vector <String> child = hash.remove(element);
                               output = addChilds(child, realNR, hash, output, element);
                        }
                        else
                        {
                               // no need to do anything it has already been added
                        }
                  }
                  
                  nextRound = realNR;
                  
           }
           
           System.out.println(output);
    }
    
    // adds all the childs and leaves it in the same state as before
    private String addChilds(Vector <String> inputVector, Vector <String> outputVector, Hashtable <String, Vector<String>> allHash, String inputString, String inputType)
    {
           for(int childIndex = 0;childIndex < inputVector.size();childIndex++)
           {
                  String child = inputVector.get(childIndex);
                  inputString = inputString + ".out().has('" + "TYPE" + "','" + child + "').as('" + child + "').in().has('" + "TYPE" + "', '" + inputType + "')";
                  if(allHash.containsKey(child))
                        outputVector.add(child);
           }
           if(outputVector.size() > 0)
                  inputString = inputString + ".out()";
           return inputString;
    }

	public GraphTraversal <Vertex, Vertex> getLastVerticesforType(String type, String instanceName)
	{
	
		// get the type for the last levelname
		String lastLevelType = headerNames[headerNames.length-1];
		GraphTraversal<Vertex, Vertex> gt8 = g.traversal().V().has(Constants.TYPE, type).has(Constants.NAME, instanceName).until(__.has(Constants.TYPE, lastLevelType)).as("b").repeat(__.out()).select("b");
		
		return gt8;
	}
	
	private static TinkerFrame load2Graph4Testing(String fileName){
		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		XSSFSheet lSheet = workbook.getSheet("Sheet1");
		
		int lastRow = lSheet.getLastRowNum();
		XSSFRow headerRow = lSheet.getRow(0);
		List<String> headerList = new ArrayList<String>();
		int totalCols = 0;
		while(headerRow.getCell(totalCols)!=null && !headerRow.getCell(totalCols).getStringCellValue().isEmpty()){
			headerList.add(headerRow.getCell(totalCols).getStringCellValue());
			totalCols++;
		}
		Map<String, Object> rowMap = new HashMap<>();
		TinkerFrame tester = new TinkerFrame();
		for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
			XSSFRow row = lSheet.getRow(rIndex);
			Object[] nextRow = new Object[totalCols];
			for(int cIndex = 0; cIndex<totalCols ; cIndex++)
			{

				int cellType = row.getCell(cIndex).getCellType();
				Object v1;
				
				if(cellType == Cell.CELL_TYPE_NUMERIC) {
					 v1 = row.getCell(cIndex).getNumericCellValue();
					 nextRow[cIndex] = v1;
				} else if(cellType == Cell.CELL_TYPE_BOOLEAN) {
					v1 = row.getCell(cIndex).getBooleanCellValue();
					nextRow[cIndex] = v1;
				} else {			
					 v1 = row.getCell(cIndex).toString();
					 nextRow[cIndex] = v1;
				}
				nextRow[cIndex] = v1;
				rowMap.put(headerList.get(cIndex), v1);
			}
			tester.addRow(nextRow, nextRow, headerList.toArray(new String[headerList.size()]));
			System.out.println("added row " + rIndex);
			System.out.println(rowMap.toString());
		}
		System.out.println("loaded file " + fileName);
		
		// 2 lines are used to create primary key table metagraph
		Map<String, Set<String>> primKeyEdgeHash = tester.createPrimKeyEdgeHash(headerList.toArray(new String[headerList.size()]));
		tester.mergeEdgeHash(primKeyEdgeHash);
		return tester;
	}
	
	/**********************   END TESTING PLAYGROUND  **************************************/
	
	
	/***********************************  CONSTRUCTORS  **********************************/
	
	public TinkerFrame(String[] headerNames) {
		
		this.headerNames = headerNames;
		g = TinkerGraph.open();
		g.createIndex(Constants.TYPE, Vertex.class);
//		g.createIndex(Constants.ID, Edge.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
	}
	
	public TinkerFrame(String[] headerNames, Hashtable<String, Set<String>> edgeHash) {
		this.headerNames = headerNames;
		g = TinkerGraph.open();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Edge.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
		mergeEdgeHash(edgeHash);
	}			 

	public TinkerFrame() {
		g = TinkerGraph.open();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Edge.class);
	}

	/*********************************  END CONSTRUCTORS  ********************************/
	
	
	/********************************  DATA MAKER METHODS ********************************/

    @Override
    public void processDataMakerComponent(DataMakerComponent component) {
           long startTime = System.currentTimeMillis();
           LOGGER.info("beginning processing of component..................................");
           processPreTransformations(component, component.getPreTrans());
           long time1 = System.currentTimeMillis();
           LOGGER.info("processing of component.................................. done with pre trans. time : " +(time1 - startTime)+" ms");
           IEngine engine = component.getEngine();
           // automatically created the query if stored as metamodel
           // fills the query with selected params if required
           // params set in insightcreatrunner
           String query = component.fillQuery();
           
           String[] displayNames = null;
           if(query.trim().toUpperCase().startsWith("CONSTRUCT")){
        	   TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
        	   tgdm.fillModel(query, engine, this);
           }
           else{
        	   ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
               //if component has data from which we can construct a meta model then construct it and merge it
               boolean hasMetaModel = component.getBuilderData() != null;
               if(hasMetaModel) {
            	   this.mergeEdgeHash(component.getBuilderData().getReturnConnectionsHash());
            	   while(wrapper.hasNext()){
            		   this.addRelationship(wrapper.next());
            	   }
               } 
               
               //else default to primary key tinker graph
               else {
                   displayNames = wrapper.getDisplayVariables();
            	   this.mergeEdgeHash(this.createPrimKeyEdgeHash(displayNames));
            	   while(wrapper.hasNext()){
            		   this.addRow(wrapper.next());
            	   }
               }
           }
           g.variables().set(Constants.HEADER_NAMES, this.headerNames); // I dont know if i even need this moving forward.. but for now I will assume it is
           redoLevels(this.headerNames);

           long time2 = System.currentTimeMillis();
           LOGGER.info("processing of component.................................. iterating through wrapper. time : " +(time2 - time1)+" ms");
           
           
           long time3 = System.currentTimeMillis();
           LOGGER.info("processing of component.................................. done iterating through wrapper. time : " +(time3 - time2)+" ms");

           processPostTransformations(component, component.getPostTrans());
           
           processActions(component, component.getActions());

           long time4 = System.currentTimeMillis();
           LOGGER.info("done processing of component................................... time : " +(time4 - time3)+" ms");
    }
	
	protected Map<String, Set<String>> createPrimKeyEdgeHash(String[] headers) {
		Set<String> primKeyEdges = new HashSet<>();
		for(String header : headers) {
			primKeyEdges.add(header);
		}
		Map<String, Set<String>> edges = new HashMap<String, Set<String>>();
		edges.put(PRIM_KEY, primKeyEdges);
		return edges;
	}

//    private void addRelationships(ISelectWrapper wrapper) {
//    	Map<String, Vertex> removeVertices = new HashMap<>();
//    	Iterator<Vertex> it = g.traversal().V().has(Constants.TYPE, "");
//    	GremlinBuilder builder = new GremlinBuilder(this.g);
//    	
//    	while(it.hasNext()) {
//    		Vertex nextVertex = it.next();
//    		removeVertices.put(nextVertex.toString(), nextVertex);
//    	}
//    	
//    	//add new relationships
////    	ISelectStatement statement = wrapper.next();
////    	Map cleanMap = statement.getPropHash();
////    	Map rawMap = statement.getRPropHash();
////    	this.addRelationship(cleanMap, rawMap);
//    	//need to do upsert here
//    	
//    	//for each remaining vertex in removeVertices, remove from graph
//    	it = g.traversal().V().has(Constants.TYPE, "");
//    	while(it.hasNext()) {
//    		Vertex nextVertex = it.next();
//    		Vertex savedVertex = removeVertices.get(nextVertex.toString());
//    		if(savedVertex != null && savedVertex.equals(nextVertex)) {
//    			it.remove();
//    		}
//    	}
//    }

/* OLD WAY	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		processPreTransformations(component, component.getPreTrans());
		
		IEngine engine = component.getEngine();
		// automatically created the query if stored as metamodel
		// fills the query with selected params if required
		// params set in insightcreatrunner
		String query = component.fillQuery();
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] displayNames = wrapper.getDisplayVariables(); // pulled this outside of the if/else block on purpose. 
		ITableDataFrame newDataFrame = null;
//		if(getHeaders() == null){
		if(this.headerNames == null) {
			g.variables().set(Constants.HEADER_NAMES, displayNames); // I dont know if i even need this moving forward.. but for now I will assume it is
			this.headerNames = displayNames;
			while(wrapper.hasNext()){
				this.addRow(wrapper.next());
			}
			// set this edge hash from component
			this.setEdgeHash(component.getBuilderData().getReturnConnectionsHash());
		}
		else {
			newDataFrame = wrapper.getTableDataFrame();

			newDataFrame.setEdgeHash(component.getBuilderData().getReturnConnectionsHash());
			// set new data frame edge hash from component
		}

		processPostTransformations(component, component.getPostTrans(), newDataFrame);
		
		processActions(component, component.getActions());

	} */

	@Override
	public void processPreTransformations(DataMakerComponent dmc,
			List<ISEMOSSTransformation> transforms) {
		LOGGER.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}
	
	@Override
	public void processPostTransformations(DataMakerComponent dmc,
			List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		LOGGER.info("We are processing " + transforms.size() + " post transformations");
		// if other data frames present, create new array with this at position 0
		IDataMaker[] extendedArray = new IDataMaker[]{this};
		if(dataFrame.length > 0) {
			extendedArray = new IDataMaker[dataFrame.length + 1];
			extendedArray[0] =  this;
			for(int i = 0; i < dataFrame.length; i++) {
				extendedArray[i+1] = dataFrame[i];
			}
		}
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
//			this.join(dataFrame, transform.getOptions().get(0).getSelected()+"", transform.getOptions().get(1).getSelected()+"", 1.0, (IAnalyticRoutine)transform);
//			LOGGER.info("welp... we've got our new table... ");
		}
		
	}

	@Override
	public Map<String, Object> getDataMakerOutput() {
		Hashtable retHash = new Hashtable();
		retHash.put("data", this.getRawData());
		retHash.put("headers", this.headerNames);
		return retHash;
	}
	
	private SEMOSSVertex getSEMOSSVertex(Map<String, SEMOSSVertex> vertStore, Vertex tinkerVert){
		Object value = tinkerVert.property(Constants.VALUE).value();
		Object type = tinkerVert.property(Constants.TYPE).value();
		// if this vertex is a literal, need to build the uri
		// otherwise the semoss vertex will not be able to get the type of the node (it parses the uri to get type)
		String uri = value + "";
		if(!(value instanceof String) || !(value instanceof String) || !((String)value).contains(((String)type))){
			uri = type + "/" + value;
		}
		SEMOSSVertex semossVert = vertStore.get(uri);
		if(semossVert == null){
			semossVert = new SEMOSSVertex(uri);
			vertStore.put(uri, semossVert);
		}
		return semossVert;
	}
	
	private Map createVertStores(){
		Map<String, SEMOSSVertex> vertStore = new HashMap<String, SEMOSSVertex>();
		Map<String, SEMOSSEdge> edgeStore = new HashMap<String, SEMOSSEdge>();
		
		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.has(T.label, META)).not(__.bothV().in().has(Constants.TYPE, Constants.FILTER));
		while(edgesIt.hasNext()){
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();
			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
			
			edgeStore.put("https://semoss.org/Relation/"+e.property(Constants.ID).value() + "", new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property(Constants.ID).value() + ""));
		}
		// now i just need to get the verts with no edges
		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.both()).not(__.has(Constants.TYPE, META)).not(__.in().has(Constants.TYPE, Constants.FILTER));
		while(vertIt.hasNext()){
			Vertex outV = vertIt.next();
			getSEMOSSVertex(vertStore, outV);
		}
		
		Map retHash = new HashMap();
		retHash.put("nodes", vertStore);
		retHash.put("edges", edgeStore.values());
		return retHash;
	}

	@Override
	public List<Object> processActions(DataMakerComponent dmc,
			List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		LOGGER.info("We are processing " + actions.size() + " actions");
		List<Object> outputs = new ArrayList<Object>();
		for(ISEMOSSAction action : actions){
			action.setDataMakers(this);
			action.setDataMakerComponent(dmc);
			outputs.add(action.runMethod());
		}
		algorithmOutput.addAll(outputs);
		return outputs;
	}
	
	@Override
	public List<Object> getActionOutput() {
		// TODO Auto-generated method stub
		return null;
	}

	/******************************  END DATA MAKER METHODS ******************************/
	
	
	
	/******************************  AGGREGATION METHODS *********************************/
	
	@Override
	public void addRow(ISelectStatement rowData) {
		// pull and add one level at a time
		addRow(rowData.getPropHash(), rowData.getRPropHash());	
	}

	@Override
	public void addRow(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {

		for(String key : rowCleanData.keySet()) {
			if(!ArrayUtilityMethods.arrayContainsValue(headerNames, key)) {
				LOGGER.error("Column name " + key + " does not exist in current tree");
			}
		}
		
		Object [] rowRawArr = new Object[headerNames.length];
		Object [] rowCleanArr = new Object[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			if(rowRawData.containsKey(headerNames[index])) {
				rowRawArr[index] = rowRawData.get(headerNames[index]).toString();
				rowCleanArr[index] = getParsedValue(rowCleanData.get(headerNames[index]));
			}
		}
		// not handling empty at this point
		addRow(rowCleanArr, rowRawArr);
	}

	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		
		getHeaders(); // why take chances.. 
		if(rowCleanData.length != headerNames.length && rowRawData.length != headerNames.length) {
			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe."); // when the HELL would this ever happen ?
		}
		
//		Vertex primVertex = upsertVertex(this.PRIM_KEY, nextPrimKey.toString(), nextPrimKey.toString());
		String rowString = "";
		Vertex[] toVertices = new Vertex[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index], rowRawData[index]); // need to discuss if we need specialized vertices too		
			toVertices[index] = toVertex;
			rowString = rowString+rowCleanData[index]+":";
		}
		
		String nextPrimKey = rowString.hashCode()+"";
		Vertex primVertex = upsertVertex(this.PRIM_KEY, nextPrimKey, nextPrimKey);
		
		for(Vertex toVertex : toVertices) {
			this.upsertEdge(primVertex, toVertex, this.PRIM_KEY);
		}
	}
	
	public void addRow(Object[] rowCleanData, Object[] rowRawData, String[] headerNames) {
		
		if(rowCleanData.length != headerNames.length && rowRawData.length != headerNames.length) {
			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe."); // when the HELL would this ever happen ?
		}
		
//		Vertex primVertex = upsertVertex(this.PRIM_KEY, nextPrimKey.toString(), nextPrimKey.toString());
		String rowString = "";
		Vertex[] toVertices = new Vertex[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index], rowRawData[index]); // need to discuss if we need specialized vertices too		
			toVertices[index] = toVertex;
			rowString = rowString+rowCleanData[index]+":";
		}
		
		String nextPrimKey = rowString.hashCode()+"";
		Vertex primVertex = upsertVertex(this.PRIM_KEY, nextPrimKey, nextPrimKey);
		
		for(Vertex toVertex : toVertices) {
			this.upsertEdge(primVertex, toVertex, this.PRIM_KEY);
		}
		
		
		//Need to update Header Names if incoming headers is different from stored header names
		if(this.headerNames == null) {
			this.headerNames = headerNames;
		} 
		
//		else {
//			
//			//see if we have any new incoming headers
//			List<String> headers = new ArrayList<String>(Arrays.asList(this.headerNames));
//			for(String header : headerNames) {
//				if(!headers.contains(header)) {
//					headers.add(header);
//				}
//			}
//			this.headerNames = headers.toArray(new String[headers.size()]);
//		}
	}
	
	public void addRelationship(ISelectStatement rowData) {
		Map<String, Object> rowCleanData = rowData.getPropHash();
		Map<String, Object> rowRawData = rowData.getRPropHash();
		addRelationship(rowCleanData, rowRawData);
	}
	
	/**
	 * Each triple as defined by the construct statement will be inserted as an edge
	 * Need to make sure the meta relationship is there and then add the instance relationship
	 * ONE CAVEAT :::: if the triple consists of all the same thing (explore an instance) its assumed to be a single node and not a relationship
	 * @param rowData
	 */
	public void addRelationship(IConstructStatement rowData) {
		String sub = rowData.getSubject();
		String subType = Utility.getClassName(sub);
		Object subInst = Utility.getInstanceName(sub);
		String pred = rowData.getPredicate();
		Object obj = rowData.getObject();
		
		// if the construct statement doesn't hold all the same thing, this means it is not the explore an instance query
		if(obj!=null && !obj.equals(sub) && ! sub.equals(pred)){
			String objType = Utility.getClassName(obj + "");
			Object objInst = Utility.getInstanceName(obj + "");
			if(objType == null || objType.isEmpty()){ // this means it is a literal
				objType = Utility.getInstanceName(pred);
				objInst = obj;
			}
			// check if meta edge has already been created for this rel
			GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, subType).out(META).has(Constants.TYPE, META).has(Constants.NAME, objType);
			if(!metaT.hasNext()){ // if it hasn't we need to add it
				Map<String, Set<String>> relMap = new HashMap<String, Set<String>>();
				Set<String> downList = new HashSet<String>();
				downList.add(objType);
				relMap.put(subType, downList);
				this.mergeEdgeHash(relMap);
			}
	
			//get from vertex
			Vertex fromVertex = upsertVertex(subType, subInst, sub);
			
			//get to vertex		
			Vertex toVertex = upsertVertex(objType, objInst, obj);
			
			upsertEdge(fromVertex, toVertex);
		}
		else {
			//check if the one meta node has been added
			GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, subType);
			if(!metaT.hasNext()){ // if it hasn't we need to add it
				Map<String, Set<String>> relMap = new HashMap<String, Set<String>>();
				Set<String> downList = new HashSet<String>();
				relMap.put(subType, downList); // add an empty set to add just the one node to meta
				this.mergeEdgeHash(relMap);
			}
			//get from vertex
			upsertVertex(subType, subInst, sub);
		}
	}
	
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {
		boolean hasRel = false;
		
		for(String startNode : rowCleanData.keySet()) {
			GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, startNode).out(META);
			while(metaT.hasNext()){
				Vertex conn = metaT.next();
				String endNode = conn.property(Constants.NAME).value()+"";
				if(rowCleanData.keySet().contains(endNode)) {
					hasRel = true;
					
					//get from vertex
					Object startNodeValue = getParsedValue(rowCleanData.get(startNode));
					String rawStartNodeValue = rowRawData.get(startNode).toString();
					Vertex fromVertex = upsertVertex(startNode, startNodeValue, rawStartNodeValue);
					
					//get to vertex		
					Object endNodeValue = getParsedValue(rowCleanData.get(endNode));
					String rawEndNodeValue = rowRawData.get(endNode).toString();
					Vertex toVertex = upsertVertex(endNode, endNodeValue, rawEndNodeValue);
					
					upsertEdge(fromVertex, toVertex);
				}
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = rowCleanData.keySet().iterator().next();
			Object startNodeValue = getParsedValue(rowCleanData.get(singleColName));
			String rawStartNodeValue = rowRawData.get(singleColName).toString();
			upsertVertex(singleColName, startNodeValue, rawStartNodeValue);
		}
	}
	
//	private void removeIncompletePaths() {
//		GraphTraversal deleteVertices = GremlinBuilder.getIncompleteVertices(getSelectors(), this.g);
//		while(deleteVertices.hasNext()) {
//			Vertex v = (Vertex)deleteVertices.next();
////			System.out.println(v.value(Constants.NAME));
////			System.out.println(v.edges(Direction.OUT).hasNext());
//			if(!(v.value(Constants.TYPE).equals(META))){
//				System.out.println(v.value(Constants.NAME));
//				if(v.edges(Direction.IN).hasNext()) {
//					System.out.println("why?");
//				}
//				if(v.edges(Direction.OUT).hasNext()) {
//					System.out.println("why2?");
//				}
//				System.out.println(v.edges(Direction.OUT).hasNext());
//				System.out.println(v.edges(Direction.IN).hasNext()); // == false)
//				v.remove();
//			} else {
//				System.out.println("HERE!");
//			}
//		}
//		
//		System.out.println("*************************************");
//		GraphTraversal totalVertices = g.traversal().V();
//		while(totalVertices.hasNext()) {
//			Vertex v = (Vertex)totalVertices.next();
//			System.out.println(v.value(Constants.NAME));
//		}
//	}

	protected Object getParsedValue(Object value) {
		Object node = null;

		if(value == null) {
		
		} else if(value instanceof Integer) {
			node = ((Number)value).intValue();
		} else if(value instanceof Number) {
			node = ((Number)value).doubleValue();
		} else if(value instanceof String) {
			node = (String)value;
		} else {
			node = value.toString();
		}
		
		return node;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
		
		// for now I am going to ignore the matcher routine
		// the incoming table has all the data that I need
		// I just need to get the graph from it and join it with the new one
		// in this case, it is coming in as the 2 columns that I want the column 1 is what I want for joining
		
		List <Object[]> output = table.getData();
		// also get the level names
		String []  joiningTableHeaders = table.getColumnHeaders();
		
		// now the job is really simple
		// I need to find the node that I want based on the headers and then add it
		for(int outIndex = 0;outIndex < output.size();outIndex++)
		{
			Object [] row = output.get(outIndex);
			Vertex v2Add = null;
			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, colNameInTable + ":" + row[0]);
			if(gt.hasNext())
			{
				v2Add = gt.next();
			}
			/*
			else // if the join type is outer then add an empty
			{
				v2Add = upsertVertex(colNameInTable, Constants.EMPTY); // create an empty
			}
			*/
			for(int colIndex = 1;colIndex < row.length;colIndex++)
			{
				// see if this exists
				// now just add everthing
				Vertex newVertex = upsertVertex(joiningTableHeaders[colIndex], row[colIndex], row[colIndex]);
				upsertEdge(v2Add, newVertex);
			}
		}
		
		// add the new set of levels
		redoLevels(joiningTableHeaders);
		mergeEdgeHash(((TinkerFrame)table).getEdgeHash()); //need more information but can assume exact string matching for now
	}
	
	private Map<String, Set<String>> getEdgeHash() {
		// Very simple -- for each meta node, get its downstream nodes and put in a set
		Map<String, Set<String>> retMap = new HashMap<String, Set<String>>();
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		while(metaT.hasNext()) {
			Vertex startNode = metaT.next();
			String startType = startNode.property(Constants.NAME).value()+"";
			Iterator<Vertex> downNodes = startNode.vertices(Direction.OUT);
			Set<String> downSet = new HashSet<String>();
			while(downNodes.hasNext()){
				Vertex downNode = downNodes.next();
				String downType = downNode.property(Constants.NAME).value()+"";
				downSet.add(downType);
			}
			retMap.put(startType, downSet);
		}
		return retMap;
	}


	/**
	 * 
	 * @param newEdgeHash
	 * 
	 * new EdgeHash in the form:
	 * 		{
	 * 			a : <b, c, d>,
	 * 			b : <x, y, z>
	 * 		}
	 * where key is column name and set is the columns key links to
	 * 
	 * This method takes the parameter edge hash information and incorporates it into the META graph incorporated within the graph
	 */
	protected void mergeEdgeHash(Map<String, Set<String>> newEdgeHash) {
		Set<String> newLevels = new LinkedHashSet<String>();
		for(String newNode : newEdgeHash.keySet()) {
			
			//grab the edges
			Set<String> edges = newEdgeHash.get(newNode);
			
			//collect the column headers
			newLevels.add(newNode);
			
			//grab/create the meta vertex associated with newNode
			Vertex outVert = upsertVertex(META, newNode, newNode);
			outVert.property(Constants.FILTER, false);
			
			//for each edge in corresponding with newNode create the connection within the META graph
			for(String inVertString : edges){
				newLevels.add(inVertString);
				
				// now to insert the meta edge
				Vertex inVert = upsertVertex(META, inVertString, inVertString);
				inVert.property(Constants.FILTER, false);
				upsertEdge(outVert, inVert, META);
			}
		}
		// need to make sure prim key is not added as header
		newLevels.remove(PRIM_KEY);
		redoLevels(newLevels.toArray(new String[newLevels.size()]));
	}
	
	/**
	 * 
	 * @param outType
	 * @param inType
	 * 
	 * Create a connection from outType to inType in the metagraph
	 */
	public void connectTypes(String outType, String inType) {

		Set<String> newLevels = new LinkedHashSet<String>();
		Vertex outVert = upsertVertex(META, outType, outType);
		outVert.property(Constants.FILTER, false);
		newLevels.add(outType);
		
		if(inType!=null){
			Vertex inVert = upsertVertex(META, inType, inType);
			inVert.property(Constants.FILTER, false);
			upsertEdge(outVert, inVert, META);
			newLevels.add(inType);
		}
		
		newLevels.remove(PRIM_KEY);
		redoLevels(newLevels.toArray(new String[newLevels.size()]));
	}
	
//	public void removeConnection(String outType, String inType) {
//		g.traversal().V().has(Constants.TYPE, META).has(Constants.VALUE, outType).outE();
//		
//		Iterator<Edge> it = g.traversal().V().has(Constants.TYPE, META).has(Constants.VALUE, outType).outE();
//		while(it.hasNext()) {
//			Edge e = it.next();
//			if(e.inVertex().value(Constants.VALUE).equals(inType)) {
//				e.remove();
//				return;
//			}
//		}
//	}

	@Override
	public void undoJoin() {
		// TODO Auto-generated method stub
		//Do we need this?
	}

	@Override
	public void append(ITableDataFrame table) {
		// TODO Auto-generated method stub
	}

	@Override
	public void undoAppend() {
		// TODO Auto-generated method stub
		//Do we need this?
	}

	/****************************** END AGGREGATION METHODS *********************************/
	@Override
	public void performAnalyticTransformation(IAnalyticTransformationRoutine routine) {
		ITableDataFrame newTable = routine.runAlgorithm(this);
		if(newTable != null) {
			this.join(newTable, newTable.getColumnHeaders()[0], newTable.getColumnHeaders()[0], 1, new ExactStringMatcher());
		}
	}

	@Override
	public List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		return null;
	}
	
	@Override
	public void performAnalyticAction(IAnalyticActionRoutine routine) {
		routine.runAlgorithm(this);
	}

	@Override
	public void undoAction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Double getEntropy(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getEntropy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getEntropyDensity(String columnHeader) {
		double entropyDensity = 0;
		
		if(isNumeric(columnHeader)) {
			//TODO: need to make barchart class better
			Double[] dataRow = getColumnAsNumeric(columnHeader);
			int numRows = dataRow.length;
			Hashtable<String, Object>[] bins = null;
			BarChart chart = new BarChart(dataRow);
			
			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("NaN", true, true);
				chart.generateJSONHashtableCategorical();
				bins = chart.getRetHashForJSON();
			} else {
				chart.generateJSONHashtableNumerical();
				bins = chart.getRetHashForJSON();
			}
			
			double entropy = 0;
			int i = 0;
			int uniqueValues = bins.length;
			for(; i < uniqueValues; i++) {
				int count = (int) bins[i].get("y");
				if(count != 0) {
					double prob = (double) count / numRows;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = (double) entropy / uniqueValues;
			
		} else {
			Map<String, Integer> uniqueValuesAndCount = getUniqueValuesAndCount(columnHeader);
			Integer[] counts = uniqueValuesAndCount.values().toArray(new Integer[]{});
			
			// if only one value, then entropy is 0
			if(counts.length == 1) {
				return entropyDensity;
			}
			
			double entropy = 0;
			double sum = StatisticsUtilityMethods.getSum(counts);
			int index;
			for(index = 0; index < counts.length; index++) {
				double val = counts[index];
				if(val != 0) {
					double prob = val / sum;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = entropy / uniqueValuesAndCount.keySet().size();
		}
		
		return -1.0 * entropyDensity;
	}

	@Override
	public Double[] getEntropyDensity() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		Integer retInt = null;
		retInt = 0;
		GraphTraversal <Vertex, Long> gt = g.traversal().V().has(Constants.TYPE, columnHeader).count();
		if(gt.hasNext())
			retInt = gt.next().intValue();

		return retInt;
	}

	@Override
	public Integer[] getUniqueInstanceCount() {
		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by(Constants.TYPE).by(__.count());
		Integer [] instanceCount = null;
		if(gt.hasNext())
		{
			Map<Object, Object> output = gt.next();
			instanceCount = new Integer[headerNames.length];
			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
				instanceCount[levelIndex] = ((Long)output.get(headerNames[levelIndex])).intValue();
		}
		return instanceCount;
	}

	@Override
	public Double getMax(String columnHeader) {
	
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).max();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

	@Override
	public Double[] getMax() {
		// unsure who wrote this, but it gives a class cast exception :/
//		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).max());
//		Double [] instanceCount = null;
//		if(gt2.hasNext())
//		{
//			Map<Object, Object> output = gt2.next();
//			instanceCount = new Double[headerNames.length];
//			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
//				instanceCount[levelIndex] = ((Number)output.get(headerNames[levelIndex])).doubleValue();
//		}
//		return instanceCount;

		int size = headerNames.length;
		Double[] max = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(headerNames[i])) {
				max[i] = getMax(headerNames[i]);
			}
		}
		
		return max;
	}

	@Override
	public Double getMin(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).min();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

	@Override
	public Double[] getMin() {

//		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).min());
//		Double [] instanceCount = null;
//		if(gt2.hasNext())
//		{
//			Map<Object, Object> output = gt2.next();
//			instanceCount = new Double[headerNames.length];
//			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
//				instanceCount[levelIndex] = ((Number)output.get(headerNames[levelIndex])).doubleValue();
//		}
//		return instanceCount;

		int size = headerNames.length;
		Double[] min = new Double[size];
		for(int i = 0; i < size; i++) {
			if(isNumeric(headerNames[i])) {
				min[i] = getMin(headerNames[i]);
			}
		}
		
		return min;
	}

	@Override
	public Double getAverage(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).mean();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

	@Override
	public Double[] getAverage() {

		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).mean());
		Double [] instanceCount = null;
		if(gt2.hasNext())
		{
			Map<Object, Object> output = gt2.next();
			instanceCount = new Double[headerNames.length];
			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
				instanceCount[levelIndex] = ((Number)output.get(headerNames[levelIndex])).doubleValue();
		}
		return instanceCount;
	}

	@Override
	public Double getSum(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).sum();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

	@Override
	public Double[] getSum() {
		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).sum());
		Double [] instanceCount = null;
		if(gt2.hasNext())
		{
			Map<Object, Object> output = gt2.next();
			instanceCount = new Double[headerNames.length];
			for(int levelIndex = 0;levelIndex < headerNames.length;levelIndex++)
				instanceCount[levelIndex] = ((Number)output.get(headerNames[levelIndex])).doubleValue();
		}
		return instanceCount;
	}

	@Override
	public Double getStandardDeviation(String columnHeader) {
		return null;
	}

	@Override
	public Double[] getStandardDeviation() {
		return null;
	}

	/**
	 * 
	 * @param columnHeader - the column we are operating on
	 * @return
	 * 
	 * returns true if values in columnHeader are numeric, false otherwise
	 * 		skips values considered 'empty'
	 */
	@Override
	public boolean isNumeric(String columnHeader) {

		//Grab from map if this value has been calculated before
		if(isNumericalMap.containsKey(columnHeader)) {
			Boolean isNum = isNumericalMap.get(columnHeader);
			if(isNum != null) {
				return isNum;
			}
		}
		
		boolean isNumeric = true;
		
		//if all values 
		Iterator<Object> iterator = this.uniqueValueIterator(columnHeader, false, false);
		while(iterator.hasNext()) {
			
			Object nextValue = iterator.next();
			if(!(nextValue instanceof Number)) {	
				//is nextValue represented by an empty?
					//if so continue
					//else store false in the isNumerical Map and break
				isNumeric = false;
				break;
			}
		}
		
		isNumericalMap.put(columnHeader, isNumeric);
		return isNumeric;
	}

	@Override
	public boolean[] isNumeric() {
		String[] headers = getHeaders();
		int size = headers.length;
		boolean[] isNumeric = new boolean[size];
		for(int i = 0; i < size; i++) {
			isNumeric[i] = isNumeric(headers[i]);
		}
		return isNumeric;
	}

	@Override
	public String[] getColumnHeaders() {
		return getHeaders();
	}
	
	protected String[] getHeaders()
	{
		if(this.headerNames == null) {
			headerNames =  (String[])(g.variables().get(Constants.HEADER_NAMES).get());
		}
		if(columnsToSkip != null && !columnsToSkip.isEmpty()) {
			List<String> headers = new ArrayList<String>();
			headers.addAll(Arrays.asList(headerNames));
			headers.removeAll(columnsToSkip);
			return headers.toArray(new String[]{});
		}
		return headerNames;
	}

	@Override
	public String[] getURIColumnHeaders() {
		return this.getHeaders();
	}

	@Override
	public int getNumCols() {
		return this.getColumnHeaders().length;
	}

	@Override
	public int getNumRows() {
		long startTime = System.currentTimeMillis();
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(getSelectors(), this.g);
		Iterator gt = builder.executeScript();		
		int count = 0;
		while(gt.hasNext()) {
			gt.next();
			count++;
		}

		long time1 = System.currentTimeMillis();
		LOGGER.info("Counted Number of Rows: " + (time1 - startTime)+" ms");
		return count;
	}

	@Override
	public int getColCount(int rowIdx) {
		return 0;
	}

	@Override
	public int getRowCount(String columnHeader) {
		
		//could use count value on edge property instead of count function?
		int retInt = 0;
		GraphTraversal<Vertex, Long> gt = g.traversal().V().has(Constants.TYPE, columnHeader).count();
		if(gt.hasNext())
		{
			retInt = gt.next().intValue();
		}
		return retInt;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		return new TinkerFrameIterator(getSelectors(), g, getRawData);
	}
	

	@Override
	public Iterator<List<Object[]>> uniqueIterator(String columnHeader,
			boolean getRawData) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object[]> standardizedIterator(boolean getRawData) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object[]> scaledIterator(boolean getRawData) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<List<Object[]>> standardizedUniqueIterator(String columnHeader, boolean getRawData) {
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData) {
		return new UniqueScaledTinkerFrameIterator(columnHeader, getRawData, getSelectors(), g, getMax(), getMin());
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {
//		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnHeader).values(Constants.VALUE);
//		return gt;
		return getGraphTraversal(columnHeader);
	}

	private GraphTraversal<Vertex, Object> getGraphTraversal(String columnHeader) {
		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnHeader).values(Constants.VALUE);
		return gt;
	}
	

	@Override
	public Object[] getColumn(String columnHeader) {
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
			List<String> selectors = new Vector<String>();
			selectors.add(columnHeader);
			
			List<Double> numericCol = new Vector<Double>();
			TinkerFrameIterator it = new TinkerFrameIterator(selectors, this.g);
			while(it.hasNext()) {
				Object[] row = it.next();
				numericCol.add( ((Number) row[0]).doubleValue() );
			}
			
			return numericCol.toArray(new Double[]{});
		}
		
		return null;
	}

	@Override
	public Object[] getRawColumn(String columnHeader) {
		return null;
	}

	@Override
	public Object[] getUniqueValues(String columnHeader) {

//		Iterator<Object> uniqIterator = this.uniqueValueIterator(columnHeader, false, false);
		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnHeader).values(Constants.VALUE);
		Vector <Object> uniV = new Vector<Object>();
		while(gt.hasNext()) {
//			Vertex v = (Vertex)uniqIterator.next();
			uniV.add(gt.next());
		}

		return uniV.toArray();
	}

	@Override
	public Object[] getUniqueRawValues(String columnHeader) {
		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnHeader).values(Constants.VALUE);
		Vector <Object> uniV = new Vector<Object>();
		while(gt.hasNext())
			uniV.add(gt.next());

		return uniV.toArray();
	}

	@Override
	public Map<String, Integer> getUniqueValuesAndCount(String columnHeader) {
		Map<String, Integer> counts = new Hashtable<String, Integer>();
		List<String> columnsToSkip = new Vector<String>();
		for(String header : headerNames) {
			if(!header.equals(columnHeader)) {
				columnsToSkip.add(header);
			}
		}
		
		TinkerFrameIterator it = new TinkerFrameIterator(getSelectors(), this.g, false);
		while(it.hasNext()) {
			Object[] row = it.next();
			if(counts.containsKey(row[0] + "")) {
				int newCount = counts.get(row[0] + "") + 1;
				counts.put(row[0] + "", newCount);
			} else {
				counts.put(row[0] + "", 1);
			}
		}
		
		return counts;
	}

	@Override
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount() {
		Map<String, Map<String, Integer>> counts = new Hashtable<String, Map<String, Integer>>();
		for(String header : headerNames) {
			counts.put(header, getUniqueValuesAndCount(header));
		}
		return counts;
	}

	@Override
	public void refresh() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		//TODO: how is this supposed to work without unfiltering the entire column first?
		//TODO: note that unfilter with a list of values method is never invoked in process flow
		unfilter(columnHeader);
		Vertex metaVertex = upsertVertex(META, columnHeader, columnHeader);
		metaVertex.property(Constants.FILTER, true);

		Vertex filterVertex = upsertVertex(Constants.FILTER, Constants.FILTER, Constants.FILTER);

		for(int i = 0 ; i < filterValues.size(); i++) {
			String id = columnHeader +":"+ filterValues.get(i);

			GraphTraversal<Vertex, Vertex> fgt = g.traversal().V().has(Constants.ID, id);
			Vertex nextVertex = null;
			if(fgt.hasNext()) {
				nextVertex = fgt.next();
				upsertEdge(filterVertex, nextVertex, Constants.FILTER);
			}
		}
	}

	@Override
	public void unfilter(String columnHeader) {
		g.traversal().V().has(Constants.TYPE, Constants.FILTER).out().has(Constants.TYPE, columnHeader).inE(Constants.FILTER).drop().iterate();
		Vertex metaVertex = this.upsertVertex(META, columnHeader, columnHeader);
		metaVertex.property(Constants.FILTER, false);
	}

	//TODO : remove the override and put it in ITableDataFrame if we need this method
	//	@Override
	public void unfilter(String columnHeader, List<Object> unfilterValues) {
		final String BIND_VAR = "bindVar";
		g.traversal().V().has(Constants.TYPE, Constants.FILTER).out().has(Constants.TYPE, columnHeader).properties(Constants.NAME).value().as(BIND_VAR)
				.where(BIND_VAR, P.within(unfilterValues.toArray(new String[]{}))).inE(Constants.FILTER);
	}

	@Override
	public void unfilter() {
		g.traversal().V().has(Constants.TYPE, Constants.FILTER).out().inE(Constants.FILTER).drop().iterate();
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, META);
		while(gt.hasNext()) {
			Vertex metaVertex = gt.next();
			metaVertex.property(Constants.FILTER, false);
		}
	}

	@Override
	public void removeColumn(String columnHeader) {
		// A couple of thoughts from Bill Sutton
		// there are quite a few interesting scenarios here
		// the first question is: do we want to maintain duplicate rows after a column is removed? I could see yes and no depending on the scenario
		// If yes, primary keys of some sort will have to be used. if the tinker already has PKs, we are good to go. Otherwise, we are probably best off just removing the column from the selectors since it will need PKs anyway
		// If no, primary keys cause a big issue--would have to remove the nodes of interest and then clean up extra PKs
		// If no and no primary keys we again have a couple scenarios. If the node is on the fringe of the tinker, good to go--just remove it. If the node is in the middle... not sure exactly what we can do--kind of similar to issue above (no and pks)

		// For now, the most common use for this will be through explore when clicking through the metamodel. This scenario will also be don't keep duplicates, no pk, node is on the fringe. I am handling that here:
		// Remove the actual nodes from tinker
		LOGGER.info("REMOVING COLUMN :::: " + columnHeader);
		// delete from the instances
		g.traversal().V().has(Constants.TYPE, columnHeader).drop().iterate();
		// remove the node from meta
		g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, columnHeader).drop().iterate();
		
		// Remove the column from header names
		String[] newHeaders = new String[this.headerNames.length-1];
		int newHeaderIdx = 0;
		for(int i = 0; i < this.headerNames.length; i++){
			String name = this.headerNames[i];
			if(!name.equals(columnHeader)){
				newHeaders[newHeaderIdx] = name;
				newHeaderIdx ++;
			}
		}
		this.headerNames = newHeaders;
	}


	@Override
	public void removeDuplicateRows() {
		//don't think this is needed
	}

	@Override
	public void removeRow(int rowIdx) {
		//unsure
	}

	@Override
	public void removeValue(String value, String rawValue, String level) {
		//do able
		//find the value, delete that node and all nodes connecting to that which have no other connections, repeat for deleted nodes
	}

	@Override
	public ITableDataFrame[] splitTableByColumn(String columnHeader) {
		return null;
		//should be 'relatively' simple to do
	}

	@Override
	public ITableDataFrame[] splitTableByRow(int rowIdx) {
		return null;
	}

	@Override
	public List<Object[]> getData() {
		
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(false);
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
		}
		return retVector;
	}

	@Override
	public List<Object[]> getAllData() {
		return null;
		//needed?
	}

	@Override
	public List<Object[]> getScaledData() {
		return null;
	}

	@Override
	public List<Object[]> getScaledData(List<String> exceptionColumns) {
		return null;
	}
	

	// Backdoor entry
	public void openBackDoor(){
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine();				
			}
		};
		thread.start();
	}
	
    /**
     * Method printAllRelationship.
     */
    public void openCommandLine()
    {
          LOGGER.warn("<<<<");
          String end = "";
          
                while(!end.equalsIgnoreCase("end"))
                {
                      try {
	                      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	                      LOGGER.info("Enter Gremlin");
	                      String query2 = reader.readLine();   
	                      if(query2!=null){
	                    	  long start = System.currentTimeMillis();
		                      end = query2;
		                      LOGGER.info("Gremlin is " + query2);
		                      GraphTraversal gt = null;
		                      try {
		                    	  GremlinGroovyScriptEngine mengine = new GremlinGroovyScriptEngine();
		                    	  mengine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", g);

		                    	  gt = (GraphTraversal)mengine.eval(end);
		                      } catch (ScriptException e) {
		                    	  e.printStackTrace();
		                      }
		                      while(gt.hasNext())
		                      {
		              			Object data = gt.next();

		              			String node = "";
		            			if(data instanceof Map) {
		            				for(Object key : ((Map)data).keySet()) {
		            					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
		            					if(mapData.get(key) instanceof Vertex){
					              			Iterator it = ((Vertex)mapData.get(key)).properties();
					              			while (it.hasNext()){
					              				node = node + it.next();
					              			}
		            					} else {
		            						node = node + mapData.get(key);
		            					}
				              			node = node + "       ::::::::::::           ";
		            				}
		            			} else {
	            					if(data instanceof Vertex){
				              			Iterator it = ((Vertex)data).properties();
				              			while (it.hasNext()){
				              				node = node + it.next();
				              			}
	            					} else {
	            						node = node + data;
	            					}
		            			}
		            			
		                        LOGGER.warn(node);
		                      }

		                      long time2 = System.currentTimeMillis();
		                      LOGGER.warn("time to execute : " + (time2 - start )+ " ms");
	                      }
                      } catch (RuntimeException e) {
                            e.printStackTrace();
                      } catch (IOException e) {
                             e.printStackTrace();
                      }
                             
                }
    }


	@Override
	public List<Object[]> getRawData() {
		
		Vector<Object[]> retVector = new Vector<>();
		Iterator<Object[]> iterator = this.iterator(true);
		while(iterator.hasNext()) {
			retVector.add(iterator.next());
		}
		return retVector;
	}

	@Override
	public List<Object[]> getData(String columnHeader, Object value) {

		// the only important piece here is columns to skip
		// and then I am not sure if I need to worry about the filtered columns
		// create the return vector
		Vector<Object[]> retVector = null;
				
		// get all the levels
		String[] headers = getHeaders();
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(getSelectors(), g);
		builder.setGroupBySelector(columnHeader);
		
		//finally execute it to get the executor
		GraphTraversal gt = (GraphTraversal) builder.executeScript();
		
		if(gt.hasNext()) {
			Map<Object, Object> groupByMap = (Map<Object, Object>) gt.next();
			List<Map> instanceArrayMap = (List<Map>) groupByMap.get(value);
			
			int size = instanceArrayMap.size();
			retVector = new Vector(size);
			for(int i = 0; i < size; i++) {
				Map rowMap = instanceArrayMap.get(i);
				Object[] row = new Object[headers.length];
				for(int j = 0; j < headers.length; j++) {
					row[j] = ((Vertex) rowMap.get(headers[j])).value(Constants.NAME);
				}
				retVector.add(row);
			}
		}

		return retVector;
	}
	
	@Override
	public boolean isEmpty() {
		return !g.traversal().V().hasNext();
		//correct?
	}

	@Override
	public void binNumericColumn(String column) {
		
	}

	@Override
	public void binNumericalColumns(String[] columns) {
		
	}

	@Override
	public void binAllNumericColumns() {
		
	}

	@Override
	public void setColumnsToSkip(List<String> columnHeaders) {
		if(columnHeaders != null)
			this.columnsToSkip = columnHeaders;
	}

	@Override
	public Object[] getFilteredUniqueRawValues(String columnHeader) {
		return null;
	}
	
	// create or add vertex
	protected Vertex upsertVertex(String type, Object data, Object value)
	{
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, type).has(Constants.ID, type + ":" + data);
		if(gt.hasNext()) {
			retVertex = gt.next();
		} else {
			//retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data, Constants.FILTER, false); //should we add a filter flag to each vertex?
			if(data instanceof Number) {
				// need to keep values as they are, not with XMLSchema tag
				retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, data, Constants.TYPE, type, Constants.NAME, data);// push the actual value as well who knows when you would need it
			} else {
				LOGGER.debug(" adding vertex ::: " + Constants.ID + " = " + type + ":" + data+ " & " + Constants.VALUE+ " = " + value+ " & " + Constants.TYPE+ " = " + type+ " & " + Constants.NAME+ " = " + data);
				retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data);// push the actual value as well who knows when you would need it
			}
		}
		return retVertex;
	}
	
	protected Edge upsertEdge(Vertex fromVertex, Vertex toVertex)
	{
		String label = fromVertex.property(Constants.ID).value() + "" + toVertex.property(Constants.ID).value();
		return upsertEdge(fromVertex, toVertex, label);
	}
	
	protected Edge upsertEdge(Vertex fromVertex, Vertex toVertex, String label) {
		Edge retEdge = null;
		String edgeID = fromVertex.property(Constants.TYPE).value() + ":" + toVertex.property(Constants.TYPE).value() + "/" + fromVertex.property(Constants.NAME).value() + ":" + toVertex.property(Constants.NAME).value();
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(Constants.ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next();
			Integer count = (Integer)retEdge.property(Constants.COUNT).value();
			count++;
			retEdge.property(Constants.COUNT, count);
		}
		else {
			retEdge = fromVertex.addEdge(label, toVertex, Constants.ID, edgeID, Constants.COUNT, 1);
		}

		return retEdge;
	}
	
	protected void redoLevels(String [] newLevels)
	{
		if(this.headerNames == null){
			this.headerNames = newLevels;
			return;
		}
		
		// put it in a set to get unique values
		Set<String> myset = new LinkedHashSet<String>(Arrays.asList(headerNames));
		myset.addAll(Arrays.asList(newLevels));
		myset.remove(PRIM_KEY);
		
		String [] newLevelNames = myset.toArray(new String[myset.size()]);

		g.variables().set(Constants.HEADER_NAMES, newLevelNames); // I dont know if i even need this moving forward.. but for now I will assume it is	
		
		String[] testHeaders = (String[])(g.variables().get(Constants.HEADER_NAMES).get());
		System.out.println(Arrays.toString(testHeaders));
		
		headerNames = newLevelNames;	
	}
	
	public void setRange(int startRange, int endRange)
	{
		this.startRange = startRange;
		this.endRange = endRange;
	}

	@Override
	/**
	 * This method returns the filter model for the graph in the form:
	 * 
	 * [
	 * 		{
	 * 			header_1 -> [UF_instance_01, UF_instance_02, ..., UF_instance_0N]
	 * 			header_2 -> [UF_instance_11, UF_instance_12, ..., UF_instance_1N]
	 * 			...
	 * 			header_M -> [UF_instance_M1, UF_instance_M2, ..., UF_instance_MN]
	 * 		}, 
	 * 
	 * 		{
	 * 			header_1 -> [F_instance_01, F_instance_02, ..., F_instance_0N]
	 * 			header_2 -> [F_instance_11, F_instance_12, ..., F_instance_1N]
	 * 			...
	 * 			header_M -> [F_instance_M1, F_instance_M2, ..., F_instance_MN]
	 * 		}	
	 * ]
	 * 
	 * the first object in the array is a Map<String, List<String>> where each header points to the list of UNFILTERED or VISIBLE values for that header
	 * the second object in the array is a Map<String, List<String>> where each header points to the list of FILTERED values for that header
	 */
	public Object[] getFilterModel() {

		Iterator<Object[]> iterator = this.iterator(true);
		
		int length = this.headerNames.length;
		
		//initialize the objects
		Map<String, List<Object>> filteredValues = new HashMap<String, List<Object>>(length);
		Map<String, List<Object>> visibleValues = new HashMap<String, List<Object>>(length);
		
		//put instances into sets to remove duplicates
		Set<Object>[] columnSets = new HashSet[length];
		for(int i = 0; i < length; i++) {
			columnSets[i] = new HashSet<Object>(length);
		}
		
		while(iterator.hasNext()) {
			Object[] nextRow = iterator.next();
			for(int i = 0; i < length; i++) {
				columnSets[i].add(nextRow[i]);
			}
		}
		
		//put the visible collected values
		for(int i = 0; i < length; i++) {
			visibleValues.put(headerNames[i], new ArrayList<Object>(columnSets[i]));
			filteredValues.put(headerNames[i], new ArrayList<Object>());
		}
		
		//grab the filter instances and put those in the second map
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, Constants.FILTER).out();
		while(gt.hasNext()) {
			Vertex value = gt.next();
			String type = value.property(Constants.TYPE).value().toString();
			List fvalues = filteredValues.get(type);
			fvalues.add(value.property(Constants.VALUE).value());
		}
		
		return new Object[]{visibleValues, filteredValues};
	}


	public Map<String, Object[]> getFilterTransformationValues() {
		Map<String, Object[]> retMap = new HashMap<String, Object[]>();
		// get meta nodes that are tagged as filtered
		GraphTraversal<Vertex, Vertex> metaGt = g.traversal().V().has(Constants.TYPE, META).has(Constants.FILTER, true);
		while(metaGt.hasNext()){
			Vertex metaV = metaGt.next();
			String vertType = metaV.property(Constants.NAME).value().toString();
			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, Constants.FILTER).out().has(Constants.TYPE, vertType);
			List<String> vertsList = new Vector<String>();
			while(gt.hasNext()){
				vertsList.add(gt.next().property(Constants.VALUE).value().toString());
			}
			retMap.put(vertType, vertsList.toArray());
		}
		
		return retMap;
	}

	public String[] getFilteredColumns() {
		return new String[0];
	}

	public Map<? extends String, ? extends Object> getGraphOutput() {
		return createVertStores();
	}
	
	public Iterator getIterator(Vector <String> columns)
	{
//		Vector <String> columnsToSkip = new Vector<String>();
//		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
//		{
//			if(!columns.contains(headerNames[colIndex]))
//				columnsToSkip.add(headerNames[colIndex]);
//		}
		
		// the columns here are the columns we want to keep
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(columns, g);
		
		return builder.executeScript();
		
	}
	
	public List<String> getSelectors() {
		List<String> selectors = new ArrayList<String>();
		for(int i = 0; i < headerNames.length; i++) {
			if(!columnsToSkip.contains(headerNames[i])) {
				selectors.add(headerNames[i]);
			}
		}
		return selectors;
	}
	
	/*
	 * a. Adding Data - nodes / relationships
	 * b. Doing some analytical routine on top of the data
	 * 	2 types here
	 * 	Map - which does for every row some calculation i.e. transformation
	 *  Reduce / Fold - which runs for all the rows i.e. Action
	 *  Or some combination of it thereof.. 
	 * c. Getting a particular set of data - some particular set of columns
	 * d. Deriving a piece of data to be added
	 * e. Getting a particular column
	 * f. Getting the rows for a particular column of data selected - special case of c for all intents and purposes
	 * g. Joining / adding a new piece of data based on existing piece of data
	 * h. Save / Read - May be we even keep this somewhere outside
	 * Given this.. can we see why we need so many methods ?
	 * 
	 */
	
	
	/**
	 * Method to generate a Tinker Frame from a JSON string
	 * Assumes all the records are unique so it assigned a unique key to each row
	 * @param jsonString				The JSON string which contains the data
	 * @return							The TinkerFrame object
	 */
	public static TinkerFrame generateTinkerFrameFromJson(String jsonString) {
		Gson gson = new Gson();
		
		List<Map<String, Object>> data = gson.fromJson(jsonString, new TypeToken<List<Map<String, Object>>>() {}.getType());
		String[] headers = data.get(0).keySet().toArray(new String[]{});
		TinkerFrame dataFrame = new TinkerFrame(headers);
		Map<String, Set<String>> primKeyEdgeHash = dataFrame.createPrimKeyEdgeHash(headers);
		dataFrame.mergeEdgeHash(primKeyEdgeHash); 
		
		int i = 0;
		int numRows = data.size();
		for(; i < numRows; i++) {
			dataFrame.addRow(data.get(i), data.get(i));
		}
		
		return dataFrame;
	}
	
	public GremlinBuilder getGremlinBuilder(List<String> selectors) {
		return GremlinBuilder.prepareGenericBuilder(selectors, g);
	}
	

	/**
	 * This method will remove all nodes that are not META and are not part of the main query return
	 * This is to keep the graph as small as possible as we are making joins
	 * Blank nodes must be used to keep nodes in the tinker that do not connect to every type
	 */
	public void removeExtraneousNodes() {
		LOGGER.info("removing extraneous nodes");
		GremlinBuilder builder = new GremlinBuilder(g);
		List<String> selectors = builder.generateFullEdgeTraversal();
		builder.addSelector(selectors);
		GraphTraversal gt = builder.executeScript();
		if(selectors.size()>1){
			gt = gt.mapValues();
		}
		GraphTraversal metaT = g.traversal().V().has(Constants.TYPE, META).outE();
		while(metaT.hasNext()){
			gt = gt.inject(metaT.next());
		}
		
		TinkerGraph newG = (TinkerGraph) gt.subgraph("subGraph").cap("subGraph").next();
		this.g = newG;
		LOGGER.info("extraneous nodes removed");
	}
	
	/**
	 * 
	 * @param fileName
	 * 
	 * serialize tinker to file
	 */
	public void save(String fileName) {
		try {
			long startTime = System.currentTimeMillis();
			this.g.io(IoCore.gryo()).writeGraph(fileName);
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param fileName
	 * @return
	 * 
	 * load tinker from file
	 */
	public static TinkerFrame open(String fileName) {
		TinkerGraph g = TinkerGraph.open();
		try {
			long startTime = System.currentTimeMillis();
			
			g.io(IoCore.gryo()).readGraph(fileName);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully loaded TinkerFrame from file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//create new tinker frame and set its tinkergraph
		TinkerFrame tf = new TinkerFrame();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Edge.class);
		tf.g = g;
		
		List<String> headersList = new Vector<String>();
		GraphTraversal<Vertex, String> hTraversal = tf.g.traversal().V().has(Constants.TYPE, META).values(Constants.NAME);
		while(hTraversal.hasNext()) {
			headersList.add(hTraversal.next());
		}
		
		//gather header names
		tf.headerNames = headersList.toArray(new String[]{});
		g.variables().set(Constants.HEADER_NAMES, tf.headerNames);
		
		return tf;
	}
	
	public Object runPKQL(String expression) {
		Parser p =
			    new Parser(
			    new Lexer(
			    new PushbackReader(
			    new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
			// new InputStreamReader(System.in), 1024)));

			   // Parse the input.
			   Start tree;
			try {
				tree = p.parse();
				   // Apply the translation.
				   tree.apply(new Translation(this));
			} catch (ParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (LexerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return this.tempExpressionResult;
	}
	
	public void setTempExpressionResult(Object result) {
		this.tempExpressionResult = result;
	}
}
