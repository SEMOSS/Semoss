package prerna.ds;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.om.TinkerGraphDataModel;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.GremlinBuilder;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerFrame extends AbstractTableDataFrame {
	
	private static final Logger LOGGER = LogManager.getLogger(TinkerFrame.class.getName());
	
	//Column Names of the table
//	protected String[] headerNames = null;
	
	//keeps the cache of whether a column is numerical or not, can this be stored on the meta model?
	protected Map<String, Boolean> isNumericalMap = new HashMap<String, Boolean>(); //store on meta
			
	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected TinkerGraph g = null;

	// stores all of the metadata
//	protected IMetaData metaData;
	
	public static final String PRIM_KEY = "_GEN_PRIM_KEY";
//	public static final String META = "META";
	public static final String EMPTY = "_";

	public static final String LIMIT = "limit";
	public static final String OFFSET = "offset";
	public static final String SELECTORS = "selectors";
	public static final String SORT_BY = "sortColumn";
	public static final String SORT_BY_DIRECTION = "sortDirection";
	public static final String DE_DUP = "dedup";
	public static final String TEMPORAL_BINDINGS = "temporalBindings";

	public static final String edgeLabelDelimeter = "+++";
	protected static final String primKeyDelimeter = ":::";

	public static final String IGNORE_FILTERS = "ignoreFilters";

		/**********************    TESTING PLAYGROUND  ******************************************/
	
	public static void main(String [] args) throws Exception
	{
//		testDeleteRows();
//		TinkerFrame t3 = new TinkerFrame();
//		testPaths();
		
		//tinkerframe to test on
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie_Data.csv";
		Map<String, Map<String, String>> dataTypeMap = new HashMap<>();
		Map<String, String> innerMap = new LinkedHashMap<>();
		
		innerMap.put("Title", "VARCHAR");
		innerMap.put("Genre", "VARCHAR");
		innerMap.put("Studio", "VARCHAR");
		innerMap.put("Director", "VARCHAR");
		
		dataTypeMap.put("CSV", innerMap);
		TinkerFrame t = (TinkerFrame) TableDataFrameFactory.generateDataFrameFromFile(fileName, ",", "Tinker", dataTypeMap, new HashMap<>());
//		TinkerFrame t = load2Graph4Testing(fileName);
		
		Iterator<Object[]> it = t.iterator(false);
		int count = 0; 
		while(it.hasNext()) {
//			System.out.println(it.next());
			it.next();
			count++;
		}
		System.out.println("COUNT IS: "+count);
		
		List<Object> list = new ArrayList<>();
		list.add("Drama");
//		list.add("Gravity");
//		list.add("Her");
//		list.add("Admission");
		t.remove("Genre", list);
		
		it = t.iterator(false);
		count = 0; 
		while(it.hasNext()) {
//			System.out.println(it.next());
			it.next();
			count++;
		}
		System.out.println("COUNT IS: "+count);
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
	
	public static void testDeleteRows() {
		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
		TinkerFrame t = load2Graph4Testing(fileName);
		
		//what i want to delete
		Map<String, Object> deleteMap = new HashMap<>();
		deleteMap.put("Studio", "Fox");
		deleteMap.put("Year", 2007.0);
		
		Iterator<Object[]> iterator = t.iterator(false);
		List<Object[]> deleteSet = new ArrayList<>();
		List<String> selectors = t.getSelectors();
		while(iterator.hasNext()) {
			boolean addRow = true;
			Object[] row = iterator.next();
			for(String column : deleteMap.keySet()) {
				int index = selectors.indexOf(column);
				if(!row[index].equals(deleteMap.get(column))) {
					addRow = false;
					break;
				}
			}
			
			if(addRow) {
				deleteSet.add(row);
				System.out.println(Arrays.toString(row));
			}
		}
		
		Set<Integer> indexes =  new HashSet<>();
		for(String column : deleteMap.keySet()) {
			indexes.add(selectors.indexOf(column));
		}
		
		for(Object[] row : deleteSet) {
			//delete all the edges necessary
			for(int i = 0; i < row.length; i++) {
				if(!indexes.contains(i)) {
					String column = selectors.get(i);
					//we have the column
					Vertex v = t.upsertVertex(column, row[i], row[i]);
					
					//we have the instance
					//how to determine what edges to delete
				}
			}
		}
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
		String fileName = "C:\\Users\\bisutton\\Desktop\\pregnancy.xlsx";
		TinkerFrame tinker = load2Graph4Testing(fileName);
		tinker.printTinker();
		
//		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(Arrays.asList(tinker.getColumnHeaders()), tinker.g);
//		GraphTraversal paths = builder.executeScript().path();
////		Object o = paths.next();
//		int count = 0;
//		while(paths.hasNext()){
//		System.out.println(paths.next());
//		count++;
//		}
//		System.out.println(count);
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
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(tinker.getSelectors(), tinker.g, ((TinkerMetaData)tinker.metaData).g, null);
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

	public void printTinker() {
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
						upsertEdge(parVertex, parentTypeName, childVertex, childTypeName);	
					}
				}
				Map <String, Set<String>> edges = new Hashtable <String, Set<String>>();
				Set set = new HashSet();
				set.add(childTypeName);
				edges.put(parentTypeName, set);
				TinkerMetaHelper.mergeEdgeHash(this.metaData, edges, null);
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
//		if(gt2.hasNext())
//			System.out.println(gt2.next());
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
				Object v1;
				if(row.getCell(cIndex)!=null){
	
					int cellType = row.getCell(cIndex).getCellType();
					
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
				else {
					nextRow[cIndex] = EMPTY;
					v1 = EMPTY;
					rowMap.put(headerList.get(cIndex), v1);
				}
			}
			tester.addRow(nextRow, nextRow, headerList.toArray(new String[headerList.size()]));
			System.out.println("added row " + rIndex);
			System.out.println(rowMap.toString());
		}
		System.out.println("loaded file " + fileName);
		
		// 2 lines are used to create primary key table metagraph
		Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headerList.toArray(new String[headerList.size()]));
		TinkerMetaHelper.mergeEdgeHash(tester.metaData, primKeyEdgeHash, null);
		return tester;
	}
	
	/**********************   END TESTING PLAYGROUND  **************************************/
	
	
	/***********************************  CONSTRUCTORS  **********************************/
	
	public TinkerFrame(String[] headerNames) {
		
		this.headerNames = headerNames;
		g = TinkerGraph.open();
//		g.createIndex(Constants.UNIQUE_NAME, Vertex.class);
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(Constants.ID, Edge.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
		this.metaData = new TinkerMetaData();
	}
	
	public TinkerFrame(String[] headerNames, Hashtable<String, Set<String>> edgeHash) {
		this.headerNames = headerNames;
		g = TinkerGraph.open();
//		g.createIndex(Constants.UNIQUE_NAME, Vertex.class);
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(Constants.ID, Edge.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
		this.metaData = new TinkerMetaData();
		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, null);
	}			 

	public TinkerFrame() {
		g = TinkerGraph.open();
//		g.createIndex(Constants.UNIQUE_NAME, Vertex.class);
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(Constants.ID, Edge.class);
		g.createIndex(T.label.toString(), Edge.class);
		this.metaData = new TinkerMetaData();
	}

	/*********************************  END CONSTRUCTORS  ********************************/
	
	
	/********************************  DATA MAKER METHODS ********************************/

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
		LOGGER.info("Processing Component..................................");

		List<ISEMOSSTransformation>  preTrans = component.getPreTrans();
		List<Map<String,String>> joinColList= new ArrayList<Map<String,String>> ();
		for(ISEMOSSTransformation transformation: preTrans){
			if(transformation instanceof JoinTransformation){
				Map<String, String> joinMap = new HashMap<String,String>();
				String joinCol1 = (String) ((JoinTransformation)transformation).getProperties().get(JoinTransformation.COLUMN_ONE_KEY);
				String joinCol2 = (String) ((JoinTransformation)transformation).getProperties().get(JoinTransformation.COLUMN_TWO_KEY);
				joinMap.put(joinCol2, joinCol1); // physical in query struct ----> logical in existing data maker
				joinColList.add(joinMap);
			}  
		}

		processPreTransformations(component, component.getPreTrans() );
		long time1 = System.currentTimeMillis();
		LOGGER.info("	Processed Pretransformations: " +(time1 - startTime)+" ms");

		IEngine engine = component.getEngine();
		// automatically created the query if stored as metamodel
		// fills the query with selected params if required
		// params set in insightcreatrunner
		String query = component.fillQuery();

		String[] displayNames = null;
		if(query.trim().toUpperCase().startsWith("CONSTRUCT")){
			TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
			tgdm.fillModel(query, engine, this);
		} else if (!query.equals(Constants.EMPTY)){
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			//if component has data from which we can construct a meta model then construct it and merge it
			boolean hasMetaModel = component.getQueryStruct() != null;
			if(hasMetaModel) {

				Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
				Map[] mergedMaps = TinkerMetaHelper.mergeQSEdgeHash(this.metaData, edgeHash, engine, joinColList);
				List<String> fullNames = this.metaData.getColumnNames();
				this.headerNames = fullNames.toArray(new String[fullNames.size()]);

				while(wrapper.hasNext()){
					ISelectStatement ss = wrapper.next();
					this.addRelationship(ss.getPropHash(), ss.getRPropHash(), mergedMaps[0], mergedMaps[1]);
				}
			} 

			//else default to primary key tinker graph
			else {
				displayNames = wrapper.getDisplayVariables();
				TinkerMetaHelper.mergeEdgeHash(this.metaData, TinkerMetaHelper.createPrimKeyEdgeHash(displayNames));
				List<String> fullNames = this.metaData.getColumnNames();
				this.headerNames = fullNames.toArray(new String[fullNames.size()]);
				while(wrapper.hasNext()){
					this.addRow(wrapper.next());
				}
			}
		}
		//           g.variables().set(Constants.HEADER_NAMES, this.headerNames); // I dont know if i even need this moving forward.. but for now I will assume it is
		//           redoLevels(this.headerNames);

		long time2 = System.currentTimeMillis();
		LOGGER.info("	Processed Wrapper: " +(time2 - time1)+" ms");

		processPostTransformations(component, component.getPostTrans());
		processActions(component, component.getActions());

		long time4 = System.currentTimeMillis();
		LOGGER.info("Component Processed: " +(time4 - startTime)+" ms");
	}
	
    /**
     * 
     * @param vertStore
     * @param tinkerVert
     * @return
     */
	private SEMOSSVertex getSEMOSSVertex(Map<String, SEMOSSVertex> vertStore, Vertex tinkerVert){
		Object value = tinkerVert.property(Constants.VALUE).value();
		String type = tinkerVert.property(Constants.TYPE).value() + "";
		
		// New logic to construct URI - don't need to take into account base URI beacuse it sits on OWL and is used upon query creation
		String newValue = Utility.getInstanceName(value.toString());
		String uri = "http://semoss.org/ontologies/Concept/" + type + "/" + newValue;
		
		// Old logic
//		if this vertex is a literal, need to build the uri
//		otherwise the semoss vertex will not be able to get the type of the node (it parses the uri to get type)
//		start with the assumption that the value is already a uri... but we can't be positive
//		let us perform some checks
//		String uri = value + "";
//		check 1
//		if it is not a value of string, most definitely need to create a uri
//		if(!(value instanceof String)) {
//			uri = "http://semoss.org/ontologies/Concept/" + type + "/" + value;
//		} else {
//			// check 2
//			// but even if it is a string, we might still need to create a uri
//			// well, if it isn't already a URI with the concatenation of the base semoss concept, a forward slash, and the type
//			// then create a uri
//			if(!value.toString().startsWith("http://semoss.org/ontologies/Concept/" + type) && !((String)value).contains(((String)type))) {
//				uri = "http://semoss.org/ontologies/Concept/" + type + "/" + value;
//			}
//		}
			
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
		
		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.or(__.has(Constants.TYPE, Constants.FILTER), __.bothV().in().has(Constants.TYPE, Constants.FILTER)));
		while(edgesIt.hasNext()){
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();
			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
			
			edgeStore.put("https://semoss.org/Relation/"+e.property(Constants.ID).value() + "", new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property(Constants.ID).value() + ""));
		}
		// now i just need to get the verts with no edges
		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.both(),__.has(Constants.TYPE, Constants.FILTER),__.in().has(Constants.TYPE, Constants.FILTER)));
		while(vertIt.hasNext()){
			Vertex outV = vertIt.next();
			getSEMOSSVertex(vertStore, outV);
		}
		
		Map retHash = new HashMap();
		retHash.put("nodes", vertStore);
		retHash.put("edges", edgeStore.values());
		return retHash;
	}
	
	private Map createVertStores2() {
		Map<String, SEMOSSVertex> vertStore = new HashMap<String, SEMOSSVertex>();
		Map<String, SEMOSSEdge> edgeStore = new HashMap<String, SEMOSSEdge>();
		
		//get all edges not attached to a filter node or is a filtered edge
		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.or(__.has(Constants.TYPE, Constants.FILTER), __.bothV().in().has(Constants.TYPE, Constants.FILTER), __.V().has(TinkerMetaData.PRIM_KEY, true)));
		while(edgesIt.hasNext()) {
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();
			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
			
			edgeStore.put("https://semoss.org/Relation/"+e.property(Constants.ID).value() + "", new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property(Constants.ID).value() + ""));
		}
		// now i just need to get the verts with no edges
//		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.both(),__.has(Constants.TYPE, Constants.FILTER),__.in().has(Constants.TYPE, Constants.FILTER)));
		
		//Not (has type filter or has in node type filter)  = not has type filter OR not has in node type filter
		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.has(Constants.TYPE, Constants.FILTER), __.in().has(Constants.TYPE, Constants.FILTER), __.has(TinkerMetaData.PRIM_KEY, true)));
//		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.in().has(Constants.TYPE, Constants.FILTER));
		while(vertIt.hasNext()) {
			Vertex outV = vertIt.next();
//			if(!outV.property("TYPE").equals(Constants.FILTER)) {
				getSEMOSSVertex(vertStore, outV);
//			}
		}
		
		
		Map retHash = new HashMap();
		retHash.put("nodes", vertStore);
		retHash.put("edges", edgeStore.values());
		return retHash;
	}
	
	/******************************  END DATA MAKER METHODS ******************************/
	
	/******************************  GRAPH SPECIFIC METHODS ******************************/


//	protected String getMetaNodeValue(String metaNodeName) {
//		String metaNodeValue = metaNodeName;
//		// get metamodel info for metaModeName
//		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.NAME, metaNodeName);
//		
//		// if metaT has metaNodeName then find the value else return metaNodeName
//		if (metaT.hasNext()) {
//			Vertex startNode = metaT.next();
//			metaNodeValue = startNode.property(Constants.VALUE).value() + "";
//		}
//
//		return metaNodeValue;
//	}
	
	/**
	 * 
	 * @param outType
	 * @param inType
	 * 
	 * Create a connection from outType to inType in the metagraph
	 */

	
	@Override
	public void connectTypes(String outType, String inType, Map<String, String> dataTypeMap) {
//		this.metaData.storeVertex(outType, outType, null);
//
//		if(inType!=null){
//			this.metaData.storeVertex(inType, inType, null);
//			this.metaData.storeRelation(outType, inType);
//		}
//
//		List<String> fullNames = this.metaData.getColumnNames();
//		this.headerNames = fullNames.toArray(new String[fullNames.size()]);

		Map<String, Set<String>> edgeHash = new HashMap<>();
		Set<String> set = new HashSet<>();
		set.add(inType);
		edgeHash.put(outType, set);
		mergeEdgeHash(edgeHash, dataTypeMap);
	}

	//TODO: need to update and remove uniqueName from method signature
	// create or add vertex
	protected Vertex upsertVertex(String type, Object data, Object value)
	{
		if(data == null) data = EMPTY;
		if(value == null) value = EMPTY;
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
		//			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, type).has(Constants.ID, type + ":" + data);
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, type + ":" + data);
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
	
	/**
	 * 
	 * @param fromVertex
	 * @param fromVertexUniqueName
	 * @param toVertex
	 * @param toVertexUniqueName
	 * @return
	 */
	protected Edge upsertEdge(Vertex fromVertex, String fromVertexUniqueName, Vertex toVertex, String toVertexUniqueName)
	{
		Edge retEdge = null;
		String type = fromVertexUniqueName + edgeLabelDelimeter + toVertexUniqueName;
		String edgeID = type + "/" + fromVertex.value(Constants.NAME) + ":" + toVertex.value(Constants.NAME);
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(Constants.ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next();
			Integer count = (Integer)retEdge.value(Constants.COUNT);
			count++;
			retEdge.property(Constants.COUNT, count);
		}
		else {
			retEdge = fromVertex.addEdge(type, toVertex, Constants.ID, edgeID, Constants.COUNT, 1);
		}

		return retEdge;
	}
	
	/******************************  END GRAPH SPECIFIC METHODS **************************/
	
	/******************************  AGGREGATION METHODS *********************************/
	
	@Override
	public void addRow(ISelectStatement rowData) {
		// pull and add one level at a time
		addRow(rowData.getPropHash(), rowData.getRPropHash());	
	}

	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		
		getColumnHeaders(); // why take chances.. 
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
		Vertex primVertex = upsertVertex(this.metaData.getLatestPrimKey(), nextPrimKey, nextPrimKey);
		
		for(int i = 0; i < headerNames.length; i++) {
			this.upsertEdge(primVertex, this.metaData.getLatestPrimKey(), toVertices[i], headerNames[i]);
		}
	}
	
	public void addRow(Object[] rowCleanData, Object[] rowRawData, String[] headerNames) {
		
		if(rowCleanData.length != headerNames.length && rowRawData.length != headerNames.length) {
			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe."); // when the HELL would this ever happen ?
		}
		
		String rowString = "";
		Vertex[] toVertices = new Vertex[headerNames.length];
		for(int index = 0; index < headerNames.length; index++) {
			Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index], rowRawData[index]); // need to discuss if we need specialized vertices too		
			toVertices[index] = toVertex;
			rowString = rowString+rowCleanData[index]+":";
		}
		
		String nextPrimKey = rowString.hashCode()+"";
		Vertex primVertex = upsertVertex(this.metaData.getLatestPrimKey(), nextPrimKey, nextPrimKey);
		
		for(int i = 0; i < toVertices.length; i++) {
			this.upsertEdge(primVertex, this.metaData.getLatestPrimKey(), toVertices[i], headerNames[i]);
		}
		
		//Need to update Header Names if incoming headers is different from stored header names
		if(this.headerNames == null) {
			this.headerNames = headerNames;
		} 
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
			GraphTraversal<Vertex, Vertex> metaT = ((TinkerMetaData)this.metaData).g.traversal().V().has(Constants.TYPE, TinkerMetaData.META).has(Constants.NAME, subType).out(TinkerMetaData.META+edgeLabelDelimeter+TinkerMetaData.META).has(Constants.TYPE, TinkerMetaData.META).has(Constants.NAME, objType);
			if(!metaT.hasNext()){ // if it hasn't we need to add it
				Map<String, Set<String>> relMap = new HashMap<String, Set<String>>();
				Set<String> downList = new HashSet<String>();
				downList.add(objType);
				relMap.put(subType, downList);
				TinkerMetaHelper.mergeEdgeHash(this.metaData, relMap);
			}
	
			//get from vertex
			Vertex fromVertex = upsertVertex(subType, subInst, sub);
			
			//get to vertex		
			Vertex toVertex = upsertVertex(objType, objInst, obj);
			
			upsertEdge(fromVertex, subType, toVertex, objType);
		}
		else {
			//check if the one meta node has been added
			GraphTraversal<Vertex, Vertex> metaT = ((TinkerMetaData)this.metaData).g.traversal().V().has(Constants.TYPE, TinkerMetaData.META).has(Constants.NAME, subType);
			if(!metaT.hasNext()){ // if it hasn't we need to add it
				Map<String, Set<String>> relMap = new HashMap<String, Set<String>>();
				Set<String> downList = new HashSet<String>();
				relMap.put(subType, downList); // add an empty set to add just the one node to meta
				TinkerMetaHelper.mergeEdgeHash(this.metaData, relMap);
			}
			//get from vertex
			upsertVertex(subType, subInst, sub);
		}
	}
	
	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
			
		boolean hasRel = false;
		
		for(String startNode : rowCleanData.keySet()) {
			Set<String> set = edgeHash.get(startNode);
			if(set==null) continue;

			for(String endNode : set) {
				if(rowCleanData.keySet().contains(endNode)) {
					hasRel = true;
					
					//get from vertex
					Object startNodeValue = getParsedValue(rowCleanData.get(startNode));
					String rawStartNodeValue = rowRawData.get(startNode).toString();
					String startNodeType = logicalToTypeMap.get(startNode);
					Vertex fromVertex = upsertVertex(startNodeType, startNodeValue, rawStartNodeValue);
					
					//get to vertex	
					Object endNodeValue = getParsedValue(rowCleanData.get(endNode));
					String rawEndNodeValue = rowRawData.get(endNode).toString();
					String endNodeType = logicalToTypeMap.get(endNode);
					Vertex toVertex = upsertVertex(endNodeType, endNodeValue, rawEndNodeValue);
					
					upsertEdge(fromVertex, startNodeType, toVertex, endNodeType);
				}
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = rowCleanData.keySet().iterator().next();
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = getParsedValue(rowCleanData.get(singleColName));
			String rawStartNodeValue = rowRawData.get(singleColName).toString();
			upsertVertex(singleNodeType, startNodeValue, rawStartNodeValue);
		}
		
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Object[] rawValues, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToTypeMap) {
		boolean hasRel = false;
		
		for(Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if(endIndices==null) continue;
			
			for(Integer endIndex : endIndices) {
				hasRel = true;
				
				//get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = logicalToTypeMap.get(headers[startIndex]);
				Object startNodeValue = getParsedValue(values[startIndex]);
				String rawStartNodeValue = values[startIndex] + "";
				Vertex fromVertex = upsertVertex(startNode, startNodeValue, rawStartNodeValue);
				
				//get to vertex	
				String endNode = headers[endIndex];
				String endUniqueName = logicalToTypeMap.get(headers[endIndex]);
				Object endNodeValue = getParsedValue(values[endIndex]);
				String rawEndNodeValue = values[endIndex] + "";
				Vertex toVertex = upsertVertex(endNode, endNodeValue, rawEndNodeValue);
				
				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = headers[0];
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = getParsedValue(values[0]);
			String rawStartNodeValue = rawValues[0] + "";
			upsertVertex(singleNodeType, startNodeValue, rawStartNodeValue);
		}
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {
		boolean hasRel = false;
		
		for(String startNode : rowCleanData.keySet()) {
			GraphTraversal<Vertex, Vertex> metaT = ((TinkerMetaData)this.metaData).g.traversal().V().has(Constants.TYPE, TinkerMetaData.META).has(Constants.NAME, startNode).out(TinkerMetaData.META+edgeLabelDelimeter+TinkerMetaData.META);
			while(metaT.hasNext()){
				Vertex conn = metaT.next();
				String endNode = conn.property(Constants.NAME).value()+"";
				if(rowCleanData.keySet().contains(endNode)) {
					hasRel = true;
					
					//get from vertex
					Object startNodeValue = getParsedValue(rowCleanData.get(startNode));
					String rawStartNodeValue = rowRawData.get(startNode).toString();
					Vertex fromVertex = upsertVertex(this.metaData.getValueForUniqueName(startNode), startNodeValue, rawStartNodeValue);
					
					//get to vertex		
					Object endNodeValue = getParsedValue(rowCleanData.get(endNode));
					String rawEndNodeValue = rowRawData.get(endNode).toString();
					Vertex toVertex = upsertVertex(this.metaData.getValueForUniqueName(endNode), endNodeValue, rawEndNodeValue);
					
					upsertEdge(fromVertex, startNode, toVertex, endNode);
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
				v2Add = upsertVertex(colNameInTable, colNameInTable, Constants.EMPTY); // create an empty
			}
			*/
			for(int colIndex = 1;colIndex < row.length;colIndex++)
			{
				// see if this exists
				// now just add everthing
				Vertex newVertex = upsertVertex(joiningTableHeaders[colIndex], row[colIndex], row[colIndex]);
				upsertEdge(v2Add, colNameInTable, newVertex, joiningTableHeaders[colIndex]);
			}
		}
		
		// add the new set of levels
		redoLevels(joiningTableHeaders);
		TinkerMetaHelper.mergeEdgeHash(this.metaData, ((TinkerFrame)table).getEdgeHash()); //need more information but can assume exact string matching for now
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

	/****************************** END AGGREGATION METHODS *********************************/
	
	/****************************** NUMERICAL/ANALYTICS METHODS *****************************/
	
	@Override
	public Double getMax(String columnHeader) {
		Double retValue = null;
		if(this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).max();
			if(gt2.hasNext()) {
				retValue = gt2.next().doubleValue();
			}
		}
		
		return retValue;
	}

	@Override
	public Double getMin(String columnHeader) {
		Double retValue = null;
		if(this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).min();
			if(gt2.hasNext()) {
				retValue = gt2.next().doubleValue();
			}
		}
		return retValue;
	}

//	@Override
	public Double getAverage(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).mean();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

//	@Override
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

//	@Override
	public Double getSum(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).sum();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

//	@Override
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
	
	/****************************** END NUMERICAL/ANALYTICS METHODS *****************************/
	
	
	/****************************** FILTER METHODS **********************************************/
	
	/**
	 * 
	 * @param columnHeader - column to remove values from
	 * @param removeValues - values to be removed
	 * 
	 * removes vertices from the graph that are associated with the column and values
	 */
	public void remove(String columnHeader, List<Object> removeValues) {
		String valueNode = this.metaData.getValueForUniqueName(columnHeader);
		
		//for each value
		for(Object val : removeValues) {
			String id = valueNode +":"+ val.toString();

			//find the vertex
			GraphTraversal<Vertex, Vertex> fgt = g.traversal().V().has(Constants.ID, id);
			Vertex nextVertex = null;
			if(fgt.hasNext()) {
				//remove
				nextVertex = fgt.next();
				nextVertex.remove();
			}
		}
	}
	
	
	/**
	 * String columnHeader - the column on which to filter on
	 * filterValues - the values that will remain in the 
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		//TODO: how is this supposed to work without unfiltering the entire column first?
		//TODO: note that unfilter with a list of values method is never invoked in process flow
		long startTime = System.currentTimeMillis();
		
		unfilter(columnHeader);
		
		Set<Object> removeSet = new HashSet<Object>();
//		Iterator<Object> iterator = uniqueValueIterator(columnHeader, false, false);
		GraphTraversal<Vertex, Vertex> iterator = g.traversal().V().has(Constants.TYPE, columnHeader);
		while(iterator.hasNext()) {
			Vertex nextVert = iterator.next();
			Object value = nextVert.property(Constants.VALUE).value();
			removeSet.add(value);
		}
		
		for(Object fv : filterValues) {
			if(fv instanceof String){
				removeSet.remove(Utility.getInstanceName((String)fv));
			}
			else {
				removeSet.remove(fv);
			}
		}
		this.metaData.setFiltered(columnHeader, true);
		String valueNode = this.metaData.getValueForUniqueName(columnHeader);
//		Vertex metaVertex = upsertVertex(META, columnHeader, valueNode);
//		metaVertex.property(Constants.FILTER, true);

		Vertex filterVertex = upsertVertex(Constants.FILTER, Constants.FILTER, Constants.FILTER);

		for(Object val : removeSet) {
			String id = valueNode +":"+ val.toString();

			GraphTraversal<Vertex, Vertex> fgt = g.traversal().V().has(Constants.ID, id);
			Vertex nextVertex = null;
			if(fgt.hasNext()) {
				nextVertex = fgt.next();
				upsertEdge(filterVertex, Constants.FILTER, nextVertex, columnHeader);
			}
		}
		
		LOGGER.info("Filtered '"+columnHeader+"':"+(System.currentTimeMillis() - startTime)+" ms");
	}
	
	@Override
	public void unfilter(String columnHeader) {
		long startTime = System.currentTimeMillis();
		
		String valueNode = this.metaData.getValueForUniqueName(columnHeader);
		g.traversal().V().has(Constants.TYPE, Constants.FILTER).out(Constants.FILTER+edgeLabelDelimeter+columnHeader).has(Constants.TYPE, valueNode).inE(Constants.FILTER+edgeLabelDelimeter+columnHeader).drop().iterate();
//		Vertex metaVertex = this.upsertVertex(META, columnHeader, valueNode);
//		metaVertex.property(Constants.FILTER, false);
		this.metaData.setFiltered(columnHeader, false);
		
		LOGGER.info("Unfiltered '"+columnHeader+"':"+(System.currentTimeMillis() - startTime)+" ms");
	}

	@Override
	public void unfilter() {
		long startTime = System.currentTimeMillis();
		g.traversal().V().has(Constants.TYPE, Constants.FILTER).outE().drop().iterate();
		this.metaData.unfilterAll();
		LOGGER.info("Unfiltered Table:"+(System.currentTimeMillis() - startTime)+" ms");
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
		int length = this.headerNames.length;
		Map<String, List<Object>> filteredValues = new HashMap<String, List<Object>>(length);
		Map<String, List<Object>> visibleValues = new HashMap<String, List<Object>>(length);
		Map<String, Map<String, Double>> minMaxValues = new HashMap<String, Map<String, Double>>(length);
		Iterator<Object[]> iterator = this.iterator(false);
		
		//put instances into sets to remove duplicates
		Set<Object>[] columnSets = new HashSet[length];
		for(int i = 0; i < length; i++) {
			columnSets[i] = new HashSet<Object>();
		}
		while(iterator.hasNext()) {
			Object[] nextRow = iterator.next();
			for(int i = 0; i < length; i++) {
				columnSets[i].add(nextRow[i]);
			}
		}
		
		for(int i = 0; i < length; i++) {
			// initialize lists for each header for unfiltered/filtered values and store data type
			visibleValues.put(headerNames[i], new ArrayList<Object>(columnSets[i]));
			filteredValues.put(headerNames[i], new ArrayList<Object>());

			// store data type for header
			// get min and max values for numerical columns
			// TODO: need to include date type
			if(this.metaData.getDataType(headerNames[i]) == IMetaData.DATA_TYPES.NUMBER) {
				Map<String, Double> minMax = new HashMap<String, Double>();

				// sort unfiltered array to pull relative min and max of unfiltered data
				Object[] unfilteredArray = columnSets[i].toArray();
				Arrays.sort(unfilteredArray);
				double absMin = getMin(headerNames[i]);
				double absMax = getMax(headerNames[i]);
				if(!columnSets[i].isEmpty()) {
					minMax.put("min", (Double)unfilteredArray[0]);
					minMax.put("max", (Double)unfilteredArray[unfilteredArray.length-1]);
				}
				minMax.put("absMin", absMin);
				minMax.put("absMax", absMax);
				
				// calculate how large each step in the slider should be
				double difference = absMax - absMin;
				double step = 1;
				if(difference < 1) {
					double tenthPower = Math.floor(Math.log10(difference));
					if(tenthPower < 0) {
						// ex. if difference is 0.009, step should be 0.001
						step = Math.pow(10, tenthPower);
					} else {
						step = 0.1;
					}
				}
				minMax.put("step", step);
				
				minMaxValues.put(headerNames[i], minMax);
			}
		}

		// begin gathering filtered data
		List<String> filterNodes = getFilteredColumns();
		for (String nameNode : filterNodes) {
			String valueName = this.metaData.getValueForUniqueName(nameNode);
			// grab the filter instances and put those in the second map
			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, Constants.FILTER).out().has(Constants.TYPE, valueName);
			while (gt.hasNext()) {
				Vertex value = gt.next();
				// get the type of the node
				List fvalues = filteredValues.get(nameNode);
				fvalues.add(value.value(Constants.NAME));
			}
		}
		return new Object[] { visibleValues, filteredValues, minMaxValues};
	}
	
	public Map<String, Object[]> getFilterTransformationValues() {
		Map<String, Object[]> retMap = new HashMap<String, Object[]>();
		// get meta nodes that are tagged as filtered
		Map<String, String> filters = this.metaData.getFilteredColumns();
		for(String name: filters.keySet()){
//			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, Constants.FILTER).out(Constants.FILTER+edgeLabelDelimeter+vertType).has(Constants.TYPE, vertType);
			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, filters.get(name));

			List<Object> vertsList = new Vector<Object>();
			while(gt.hasNext()){
//				System.out.println(gt.next());
				Vertex vert = gt.next();
//				System.out.println(vert.value(Constants.VALUE) + "");
				if(!vert.edges(Direction.IN, Constants.FILTER+edgeLabelDelimeter+filters.get(name)).hasNext()) {
					vertsList.add(vert.value(Constants.VALUE));
				}
			}
			retMap.put(name, vertsList.toArray());
		}
		
		return retMap;
	}
	
	
	@Override
	public void filter(String columnHeader, Map<String, List<Object>> filterData) {
		
		unfilter(columnHeader);
		
		QueryStruct qs = new QueryStruct();
		qs.addSelector(columnHeader, null);
		for(String comparator : filterData.keySet()) {
			qs.addFilter(columnHeader, comparator, filterData.get(comparator));
		}
		
		GremlinInterpreter interpreter = new GremlinInterpreter(this.g, ((TinkerMetaData)this.metaData).g);
		interpreter.setQueryStruct(qs);
		GraphTraversal it = (GraphTraversal)interpreter.composeIterator();
		List<Object> filterList = new ArrayList<>();
		while(it.hasNext()) {
			filterList.add(((Vertex)it.next()).value(Constants.NAME));
		}
		
		filter(columnHeader, filterList);
	}
	
	/****************************** END FILTER METHODS ******************************************/
	
	
	/****************************** TRAVERSAL METHODS *******************************************/
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(TinkerFrame.SELECTORS, getSelectors());
		return new TinkerFrameIterator(g, ((TinkerMetaData)this.metaData).g, options, getRawData);
	}
	
	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		return new TinkerFrameIterator(g, ((TinkerMetaData)this.metaData).g, options, getRawData);
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData, Map<String, Object> options) {
		List<String> selectors = null;
		Double[] max = null;
		Double[] min = null;
		if(options.containsKey(TinkerFrame.SELECTORS)) {
			selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
			int numSelected = selectors.size();
			max = new Double[numSelected];
			min = new Double[numSelected];
			for(int i = 0; i < numSelected; i++) {
				//TODO: think about storing this value s.t. we do not need to calculate max/min with each loop
				max[i] = getMax(selectors.get(i));
				min[i] = getMin(selectors.get(i));
			}
		} else {
			selectors = getSelectors();
			max = getMax();
			min = getMin();
		}
		
		return new UniqueScaledTinkerFrameIterator(columnHeader, getRawData, selectors, g, ((TinkerMetaData)this.metaData).g, max, min);
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {

//		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, columnHeader);
//		if(!iterateAll){
//			gt = gt.not(__.in().has(Constants.TYPE, Constants.FILTER)); //TODO: this isn't exactly right.. doesnt' handle transitive filters..
//		}
//		return gt.values(Constants.NAME);
		
		Vector<String> column = new Vector<>();
		column.add(columnHeader);
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(column, g, ((TinkerMetaData)this.metaData).g, null);
		String dataType = getRawData ? Constants.VALUE : Constants.NAME;
		return builder.executeScript().values(dataType).dedup();	
	}

	private GraphTraversal<Vertex, Object> getGraphTraversal(String columnHeader) {
		String columnType = this.metaData.getValueForUniqueName(columnHeader);
		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnType).values(Constants.VALUE);
		return gt;
	}
	
	public Iterator getIterator(Vector <String> columns) {
		// the columns here are the columns we want to keep
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(columns, g, ((TinkerMetaData)this.metaData).g, null);
		return builder.executeScript();	
	}
	
	/****************************** END TRAVERSAL METHODS ***************************************/
	
	
//	/**
//	 * 
//	 * @param columnHeader - the column we are operating on
//	 * @return
//	 * 
//	 * returns true if values in columnHeader are numeric, false otherwise
//	 * 		skips values considered 'empty'
//	 */
//	@Override
//	public boolean isNumeric(String columnHeader) {
//
//		//Grab from map if this value has been calculated before
//		if(isNumericalMap.containsKey(columnHeader)) {
//			Boolean isNum = isNumericalMap.get(columnHeader);
//			if(isNum != null) {
//				return isNum;
//			}
//		}
//		
//		boolean isNumeric = true;
//		
//		//if all values 
//		Iterator<Object> iterator = this.uniqueValueIterator(columnHeader, false, false);
//		while(iterator.hasNext()) {
//			
//			Object nextValue = iterator.next();
//			if(!(nextValue instanceof Number)) {	
//				//is nextValue represented by an empty?
//					//if so continue
//					//else store false in the isNumerical Map and break
//				isNumeric = false;
//				break;
//			}
//		}
//		
//		isNumericalMap.put(columnHeader, isNumeric);
//		return isNumeric;
//	}

	@Override
	public int getNumRows() {
		long startTime = System.currentTimeMillis();
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(getSelectors(), this.g, ((TinkerMetaData)this.metaData).g, null);
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

//	@Override
//	public int getRowCount(String columnHeader) {
//		
//		//could use count value on edge property instead of count function?
//		int retInt = 0;
//		String columnValue = this.metaData.getValueForUniqueName(columnHeader);
//		GraphTraversal<Vertex, Long> gt = g.traversal().V().has(Constants.TYPE, columnValue).count();
//		if(gt.hasNext())
//		{
//			retInt = gt.next().intValue();
//		}
//		return retInt;
//	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
			List<String> selectors = new Vector<String>();
			selectors.add(columnHeader);
			Map<String, Object> options = new HashMap<String, Object>();
			options.put(TinkerFrame.SELECTORS, selectors);
			List<Double> numericCol = new Vector<Double>();
			Iterator<Object[]> it = iterator(false, options);
			while(it.hasNext()) {
				Object[] row = it.next();
				numericCol.add( ((Number) row[0]).doubleValue() );
			}
			
			return numericCol.toArray(new Double[]{});
		}
		
		return null;
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		// need to iterate through everything since we do not clean up vertices
		Vector<String> selectors = new Vector<String>();
		selectors.add(columnHeader);
		Iterator it = this.getIterator(selectors);
		Set<Object> columnSet = new HashSet<Object>();
		while(it.hasNext()) {
			columnSet.add(it.next());
		}
		
		return columnSet.size();
//		GraphTraversal <Vertex, Long> gt = g.traversal().V().has(Constants.TYPE, columnHeader).count();
//		if(gt.hasNext())
//			retInt = gt.next().intValue();
//
//		return retInt;
	}

//	@Override
	public Object[] getUniqueValues(String columnHeader) {

		Iterator<Object> uniqIterator = this.uniqueValueIterator(columnHeader, false, false);
//		GraphTraversal<Vertex, Object> gt = g.traversal().V().has(Constants.TYPE, columnHeader).values(Constants.VALUE);
		Vector <Object> uniV = new Vector<Object>();
		while(uniqIterator.hasNext()) {
//			Vertex v = (Vertex)uniqIterator.next();
			uniV.add(uniqIterator.next());
		}

		return uniV.toArray();
	}

	@Override
	public void removeColumn(String columnHeader) {
		// if column header doesn't exist, do nothing
		if(!ArrayUtilityMethods.arrayContainsValue(this.headerNames, columnHeader)) {
			return;
		}
		
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
		
		//if columnHeader has incoming prim key with no other outgoing types, delete that prim key first, then delete the columnHeader
		
//		g.traversal().V().has(Constants.TYPE, PRIM_KEY).as("PrimKey").out().has(Constants.TYPE, columnHeader).in().has(Constants.TYPE, PRIM_KEY).as("PrimKey2").where("PrimKey", P.eq("PrimKey2")).not(__.out().has(Constants.TYPE, columnHeader));
		String columnValue = this.metaData.getValueForUniqueName(columnHeader);
		GraphTraversal<Vertex, Vertex> primKeyTraversal = g.traversal().V().has(Constants.VALUE, PRIM_KEY).as("PrimKey").out(PRIM_KEY+edgeLabelDelimeter+columnValue).has(Constants.TYPE, columnValue).in(PRIM_KEY+edgeLabelDelimeter+columnValue).has(Constants.VALUE, PRIM_KEY).as("PrimKey2").where("PrimKey", P.eq("PrimKey2"));
		while(primKeyTraversal.hasNext()) {
			Vertex nextPrimKey = (Vertex)primKeyTraversal.next();
			Iterator<Vertex> verts = nextPrimKey.vertices(Direction.OUT);
			
			boolean delete = true;
			while(verts.hasNext()) {
				delete = verts.next().value(Constants.TYPE).equals(columnHeader);
				if(!delete) {
					delete = false;
					break;
				}
			}
			if(delete) {
				nextPrimKey.remove();
			}
		}
		
//		GraphTraversal<Vertex, Vertex> metaPrimKeyTraversal = g.traversal().V().has(Constants.TYPE, META).has(Constants.VALUE, PRIM_KEY).as("PrimKey").out(PRIM_KEY+edgeLabelDelimeter+columnValue).has(Constants.NAME, columnHeader).in(PRIM_KEY+edgeLabelDelimeter+columnValue).has(Constants.VALUE, PRIM_KEY).as("PrimKey2").where("PrimKey", P.eq("PrimKey2"));
////		GraphTraversal<Vertex, Vertex> primKeyTraversal = g.traversal().V().has(Constants.TYPE, META).as("PrimKey").out().has(Constants.TYPE, columnHeader).in().has(Constants.VALUE, PRIM_KEY).as("PrimKey2").where("PrimKey", P.eq("PrimKey2"));
//		while(metaPrimKeyTraversal.hasNext()) {
//			Vertex metaPrimKey = (Vertex)metaPrimKeyTraversal.next();
//			Iterator<Vertex> verts = metaPrimKey.vertices(Direction.OUT);
//			
//			boolean delete = true;
//			while(verts.hasNext()) {
//				delete = verts.next().value(Constants.NAME).equals(columnHeader);
//				if(!delete) {
//					delete = false;
//					break;
//				}
//			}
//			if(delete) {
//				metaPrimKey.remove();
//			}
//		}
		
		
		g.traversal().V().has(Constants.TYPE, columnValue).drop().iterate();
		// remove the node from meta
//		g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, columnHeader).drop().iterate();
		this.metaData.dropVertex(columnHeader);
		
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
	
	public GraphTraversal runGremlin (String gremlinQuery){
		//instead of running the openCommandLine we are going to specify the query that we want to return data for. 
		GraphTraversal gt = null;
		try {
			if(gremlinQuery!=null){
				long start = System.currentTimeMillis();
				LOGGER.info("Gremlin is " + gremlinQuery);
				try {
					GremlinGroovyScriptEngine mengine = new GremlinGroovyScriptEngine();
					mengine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", g);

					gt = (GraphTraversal)mengine.eval(gremlinQuery);
					System.out.println("compiled gremlin :: " + gt);
				} catch (ScriptException e) {
					e.printStackTrace();
				}
//				if(gt.hasNext()) {
//					Object data = gt.next();
//
//					String node = "";
//					if(data instanceof Map) {
//						for(Object key : ((Map)data).keySet()) {
//							Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
//							if(mapData.get(key) instanceof Vertex){
//								Iterator it = ((Vertex)mapData.get(key)).properties();
//								while (it.hasNext()){
//									node = node + it.next();
//								}
//							} else {
//								node = node + mapData.get(key);
//							}
//							node = node + "       ::::::::::::           ";
//						}
//					} else {
//						if(data instanceof Vertex){
//							Iterator it = ((Vertex)data).properties();
//							while (it.hasNext()){
//								node = node + it.next();
//							}
//						} else {
//							node = node + data;
//						}
//					}
//
//					LOGGER.warn(node);
//				}
//
//				long time2 = System.currentTimeMillis();
//				LOGGER.warn("time to execute : " + (time2 - start )+ " ms");
//				return gt;
//			}
			
		}} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return gt;
		

	}
	
	public Object degree(String type, String data)
	{
		GraphTraversal <Vertex, Map<Object, Object>> gt = g.traversal().V().has("ID", type + ":" + data).group().by().by(__.bothE().count());
		Object degree = null;
		if(gt.hasNext())
		{
			Map <Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while(mapKeys.hasNext())
			{
				Object key = mapKeys.next();
				Object value = map.get(key);
				degree = value;
				
				System.out.println(((Vertex)key).value("ID") + "<<>>" + value);				
			}			
		}
		return degree;
	}
	
	
	public Long eigen(String type, String data)
	{
		Long retLong = null;
		GraphTraversal<Vertex, Map<String, Object>> gt2 = g.traversal().V().repeat(__.groupCount("m").by("ID").out()).times(5).cap("m")
				.V()
				//.has("ID", type + ":" + data)
				.select("m");
				//.where("V);
		if(gt2.hasNext())
		{
			Map <String, Object> map = gt2.next();
			retLong = (Long)map.get(type + ":" +  data);
			System.out.println(retLong);
		}
		
		return retLong;
	}

	public void printEigenMatrix()
	{
		GraphTraversal <Vertex, Map<Object, Object>> gt = g.traversal().V().repeat(__.groupCount("m").by("ID").out()).times(5).cap("m"); //. //(1)
        //order(Scope.local).by(__.values(), Order.decr).limit(Scope.local, 10); //.next(); //(2)
		if(gt.hasNext())
		{
			Map <Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while(mapKeys.hasNext())
			{
				Object key = mapKeys.next();
				Object value = map.get(key);
				System.out.println(key + "<<>>" + value);				
				//System.out.println(((Vertex)key).value("ID") + "<<>>" + value);
			}			
		}
	}

	
	public boolean isOrphan(String type, String data)
	{
		boolean retValue = false;
		
		GraphTraversal<Vertex, Edge> gt = g.traversal().V().has("ID", type + ":" + data).bothE();
		if(gt.hasNext())
		{
			System.out.println(data + "  Not Orphan");
			retValue = false;
		}
		else
		{
			System.out.println(data + "  is Orphan");
			retValue = true;
		}
		
		return retValue;
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
		                    	  System.out.println("compiled gremlin :: " + gt);
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

//	@Override
//	public List<Object[]> getData(String columnHeader, Object value) {
//
//		// the only important piece here is columns to skip
//		// and then I am not sure if I need to worry about the filtered columns
//		// create the return vector
//		Vector<Object[]> retVector = null;
//				
//		// get all the levels
//		String[] headers = getColumnHeaders();
//		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(getSelectors(), g, ((TinkerMetaData)this.metaData).g, null);
//		builder.setGroupBySelector(columnHeader);
//		
//		//finally execute it to get the executor
//		GraphTraversal gt = (GraphTraversal) builder.executeScript();
//		
//		if(gt.hasNext()) {
//			Map<Object, Object> groupByMap = (Map<Object, Object>) gt.next();
//			List<Map> instanceArrayMap = (List<Map>) groupByMap.get(value);
//			
//			int size = instanceArrayMap.size();
//			retVector = new Vector(size);
//			for(int i = 0; i < size; i++) {
//				Map rowMap = instanceArrayMap.get(i);
//				Object[] row = new Object[headers.length];
//				for(int j = 0; j < headers.length; j++) {
//					row[j] = ((Vertex) rowMap.get(headers[j])).value(Constants.NAME);
//				}
//				retVector.add(row);
//			}
//		}
//
//		return retVector;
//	}


	protected void redoLevels(String [] newLevels)
	{
		if(this.headerNames == null){
			this.headerNames = newLevels;
			return;
		}
		
		// put it in a set to get unique values
		Set<String> myset = new LinkedHashSet<String>(Arrays.asList(headerNames));
		
		for(String newLevel : newLevels) {
			if(!newLevel.contains(primKeyDelimeter)) {
				myset.add(newLevel);
			}
		}
//		myset.remove(PRIM_KEY);
		
		String [] newLevelNames = myset.toArray(new String[myset.size()]);

		g.variables().set(Constants.HEADER_NAMES, newLevelNames); // I dont know if i even need this moving forward.. but for now I will assume it is	
		
		String[] testHeaders = (String[])(g.variables().get(Constants.HEADER_NAMES).get());
		System.out.println(Arrays.toString(testHeaders));
		
		headerNames = newLevelNames;	
	}

	/**
     * Get all meta nodes with filters equaling a true boolean
     */
	public List<String> getFilteredColumns() {
		return new ArrayList<String>(this.metaData.getFilteredColumns().keySet());
	}

	public Map<? extends String, ? extends Object> getGraphOutput() {
		//return createVertStores();
		return createVertStores2();
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
	 * This method will remove all nodes that are not META and are not part of the main query return
	 * This is to keep the graph as small as possible as we are making joins
	 * Blank nodes must be used to keep nodes in the tinker that do not connect to every type
	 */
	public void removeExtraneousNodes() {
//		LOGGER.info("removing extraneous nodes");
//		GremlinBuilder builder = new GremlinBuilder(g);
//		List<String> selectors = builder.generateFullEdgeTraversal();
//		builder.addSelector(selectors);
//		GraphTraversal gt = builder.executeScript();
//		if(selectors.size()>1){
//			gt = gt.mapValues();
//		}
//		GraphTraversal metaT = g.traversal().V().has(Constants.TYPE, META).outE();
//		while(metaT.hasNext()){
//			gt = gt.inject(metaT.next());
//		}
//		
//		TinkerGraph newG = (TinkerGraph) gt.subgraph("subGraph").cap("subGraph").next();
//		this.g = newG;
//		LOGGER.info("extraneous nodes removed");
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
			this.removeExtraneousNodes();
			
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();;
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.writeGraph(fileName);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.metaData.save(fileName.substring(0, fileName.lastIndexOf(".")));
	}
	
	/**
	 * Open a serialized TinkerFrame
	 * This is used with in InsightCache class
	 * @param fileName				The file location to the cached graph
	 * @param userId				The userId who is creating this instance of the frame
	 * @return
	 */
	public TinkerFrame open(String fileName, String userId) {
		// create a new tinker frame class
		TinkerFrame tf = new TinkerFrame();
		// set the user id
		tf.setUserId(userId);
		try {
			long startTime = System.currentTimeMillis();

			// user kyro to de-serialize the cached graph
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(tf.g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(fileName);

			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully loaded TinkerFrame from file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
		// need to also set the metaData
		// the meta data fileName parameter passed is going to be the same as the name as the file of the actual instances
		// this isn't the actual fileName of the file, the metadata appends the predefined prefix for the file
		tf.metaData.open(fileName.substring(0, fileName.lastIndexOf(".")));
		// set the list of headers in the class variable
		List<String> fullNames = tf.metaData.getColumnNames();
		tf.headerNames = fullNames.toArray(new String[fullNames.size()]);

		// return the new instance
		return tf;
	}

	public void insertBlanks(String colName, List<String> addedColumns) {
		// for each node in colName
		// if it does not have a relationship to any node in any of the addedColumns
		// add that node to a blank
		LOGGER.info("PERFORMING inserting of blanks.......");
		for(String addedType : addedColumns){
			Vertex emptyV = null;
			String colValue = this.metaData.getValueForUniqueName(colName);
			String addedValue = this.metaData.getValueForUniqueName(addedType);
			boolean forward = false;
			GraphTraversal<Vertex, Vertex> gt = null;
			if(this.metaData.isConnectedInDirection(colValue, addedType)){
				forward = true;
				gt = g.traversal().V().has(Constants.TYPE, colValue).not(__.out(colValue+edgeLabelDelimeter+addedValue).has(Constants.TYPE, addedValue));
			}
			else {
				gt = g.traversal().V().has(Constants.TYPE, colValue).not(__.in(addedValue+edgeLabelDelimeter+colValue).has(Constants.TYPE, addedValue));
			}
			while(gt.hasNext()){ // these are the dudes that need an empty
				if(emptyV == null){
					emptyV = this.upsertVertex(addedValue, EMPTY, EMPTY);
				}
				
				Vertex existingVert = gt.next();
				if(forward){
					this.upsertEdge(existingVert, colName, emptyV, addedType); 
				}
				else {
					this.upsertEdge(emptyV, addedType, existingVert, colName); 
				}
			}
		}
		LOGGER.info("DONE inserting of blanks.......");
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.MATH_PARAM, "prerna.sablecc.MathParamReactor");
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.TinkerColAddReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.TinkerImportDataReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");

//		switch(reactorType) {
//			case IMPORT_DATA : return new TinkerImportDataReactor();
//			case COL_ADD : return new TinkerColAddReactor();
//		}
		
		return reactorNames;
	}
	
	@Override
	public String getDataMakerName() {
		return "TinkerFrame";
	}
}
