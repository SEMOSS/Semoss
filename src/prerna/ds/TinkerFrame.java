package prerna.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.GremlinBuilder;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;

public class TinkerFrame implements ITableDataFrame {
	
	private static final Logger LOGGER = LogManager.getLogger(TinkerFrame.class.getName());
	
	//Column Names of the table
	private String [] headerNames = null;
	private List<String> columnsToSkip = new Vector<String>(); //make a set?
	
	//keeps the values that are filtered from the table
	Hashtable <String, List<Object>> filterHash = new Hashtable<String, List<Object>>();
	
	//keeps the cache of whether a column is numerical or not
	Map<String, Boolean> isNumericalMap = new HashMap<String, Boolean>();
	
	//keeps the cache of edges within the tree
	Map <String, Set<String>> edgeHash = new Hashtable<String, Set<String>>();
	
//	Graph metamodel = null;
	
	protected List<Object> algorithmOutput = new Vector<Object>();
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	TinkerGraph g = null;
	int startRange = -1;
	int endRange = -1;

	/**********************    TESTING PLAYGROUND  ******************************************/
	
	public static void main(String [] args) throws Exception
	{
		
		TinkerFrame t3 = new TinkerFrame();
		//t3.writeToFile();
		//t3.readFromFile();
		//t3.doTest();
//		t3.tryCustomGraph();
		t3.tryFraph();
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
		if(gt.hasNext())
			System.out.println(gt.next());
		System.out.println("Completed group by");
		
		System.out.println("Trying max");
		GraphTraversal<Vertex, Number> gt2 = g.traversal().V().has("TYPE", "Activity").values("DATA").max();
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

		getRawData();
	}

	private void tryBuilder()
	{
		
		
		
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
		//GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", P.not(org.apache.tinkerpop.gremlin.process.traversal.P.within("marko", "ripple")));
		String [] names = {"marko", "ripple"};
		GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", org.apache.tinkerpop.gremlin.process.traversal.P.without(names));
		
		while(gt11.hasNext())
		{
			System.out.println(gt11.next());
		}
		System.out.println("Now printing from partition");
		GraphTraversalSource newGraph = GraphTraversalSource.build().with(stratA).create(g);
		gt11 = newGraph.V().has("name", org.apache.tinkerpop.gremlin.process.traversal.P.within("marko"));
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
	
	/**********************   END TESTING PLAYGROUND  **************************************/
	
	
	/***********************************  CONSTRUCTORS  **********************************/
	
	public TinkerFrame(String[] headerNames) {
		//should we define header names in constructor
		//do we need a filtered columns array?
		//do we need to keep track of URIs?
		
		this.headerNames = headerNames;
		g = TinkerGraph.open();
		g.createIndex(Constants.ID, Vertex.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
	}
	
	public TinkerFrame(String[] headerNames, Hashtable<String, Set<String>> edgeHash) {
		this.headerNames = headerNames;
		this.edgeHash = edgeHash;
		g = TinkerGraph.open();
		g.createIndex(Constants.ID, Vertex.class);
		g.variables().set(Constants.HEADER_NAMES, headerNames);
	}			 

	public TinkerFrame() {
		g = TinkerGraph.open();
		g.createIndex(Constants.ID, Vertex.class);
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
           
           ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
           String[] displayNames = wrapper.getDisplayVariables(); // pulled this outside of the if/else block on purpose. 
//         ITableDataFrame newDataFrame = null;
//         if(getHeaders() == null){
//         if(this.headerNames == null) {
           
           // set this edge hash from component
           this.mergeEdgeHash(component.getBuilderData().getReturnConnectionsHash());
           
           g.variables().set(Constants.HEADER_NAMES, displayNames); // I dont know if i even need this moving forward.. but for now I will assume it is
           redoLevels(displayNames);

           long time2 = System.currentTimeMillis();
           LOGGER.info("processing of component.................................. iterating through wrapper. time : " +(time2 - time1)+" ms");
           while(wrapper.hasNext()){
//                this.addRow(wrapper.next());
                  this.addRelationship(wrapper.next());
           }
           long time3 = System.currentTimeMillis();
           LOGGER.info("processing of component.................................. done iterating through wrapper. time : " +(time3 - time2)+" ms");

//         }
//         else {
//                newDataFrame = wrapper.getTableDataFrame();
//
//                newDataFrame.setEdgeHash(component.getBuilderData().getReturnConnectionsHash());
//                // set new data frame edge hash from component
//         }

           processPostTransformations(component, component.getPostTrans(), this);
           
           processActions(component, component.getActions());

           long time4 = System.currentTimeMillis();
           LOGGER.info("done processing of component................................... time : " +(time4 - time3)+" ms");
    }


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
			if(rowRawData.containsKey(headerNames[index])){
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
		
		// dont believe me just watch
		for(int index = 0; index < headerNames.length; index++) {
			if(rowCleanData[index] != null){
				Vertex fromVertex = upsertVertex(headerNames[index], rowCleanData[index]+"", rowRawData[index]); // need to discuss if we need specialized vertices too
				int parentInc = 1;
				boolean added = false;
				while(index + parentInc < headerNames.length && !added)
				{
					if(rowCleanData[index+parentInc] == null){
						parentInc ++;
						continue;
					}
					Vertex toVertex = upsertVertex(headerNames[index+parentInc], rowCleanData[index+parentInc]+"", rowRawData[index+parentInc]);
					upsertEdge(fromVertex, toVertex);
					added = true;
				}
			}
		}
		// and tada.. it is gone !!
	}
	
	public void addRelationship(ISelectStatement rowData) {
		Map<String, Object> rowCleanData = rowData.getPropHash();
		Map<String, Object> rowRawData = rowData.getRPropHash();
		
		boolean hasRel = false;
		for(String startNode : rowCleanData.keySet()) {
			Set<String> set = this.edgeHash.get(startNode);
			if(set==null) continue;
//			String endNode = rowCleanData.get(key).toString();
			for(String endNode : set) {
				if(rowCleanData.keySet().contains(endNode)) {
					hasRel = true;
					//get from vertex
					Object startNodeValue = getParsedValue(rowCleanData.get(startNode));
					String rawStartNodeValue = rowRawData.get(startNode).toString();
					Vertex fromVertex = upsertVertex(startNode, startNodeValue+"", rawStartNodeValue);
					//get to vertex
							
					Object endNodeValue = getParsedValue(rowCleanData.get(endNode));
					String rawEndNodeValue = rowRawData.get(endNode).toString();
					Vertex toVertex = upsertVertex(endNode, endNodeValue+"", rawEndNodeValue);
					
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
			upsertVertex(singleColName, startNodeValue+"", rawStartNodeValue);
		}
		
//		Object [] rowRawArr = new Object[headerNames.length];
//		Object [] rowCleanArr = new Object[headerNames.length];
//		for(int index = 0; index < headerNames.length; index++) {
//			if(rowRawData.containsKey(headerNames[index])){
//				rowRawArr[index] = rowRawData.get(headerNames[index]).toString();
//				rowCleanArr[index] = getParsedValue(rowCleanData.get(headerNames[index]));
//			}
//		}
		// not handling empty at this point
//		addRow(rowCleanArr, rowRawArr);
	}

	private Object getParsedValue(Object value) {
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
				Vertex newVertex = upsertVertex(joiningTableHeaders[colIndex], row[colIndex]+"", row[colIndex]);
				upsertEdge(v2Add, newVertex);
			}
		}
		
		// add the new set of levels
		redoLevels(joiningTableHeaders);
		mergeEdgeHash(table.getEdgeHash()); //need more information but can assume exact string matching for now
	}
	
	private void mergeEdgeHash(Map<String, Set<String>> newEdgeHash) {
		for(String newNode : newEdgeHash.keySet()) {
			Set<String> edges = newEdgeHash.get(newNode);
			if(this.edgeHash.get(newNode) == null) {
				this.edgeHash.put(newNode, edges);
			} else {
				this.edgeHash.get(newNode).addAll(edges);
			}
		}
	}

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
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		return null;
	}
	
	@Override
	public void performAnalyticAction(IAnalyticActionRoutine routine) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		return null;
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
		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).max());
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
	public Double getMin(String columnHeader) {
		Double retValue = null;
		GraphTraversal<Vertex, Number> gt2 = getGraphTraversal(columnHeader).min();
		if(gt2.hasNext())
			retValue = gt2.next().doubleValue();
		
		return retValue;
	}

	@Override
	public Double[] getMin() {

		GraphTraversal<Vertex, Map<Object, Object>> gt2 =  g.traversal().V().group().by(Constants.TYPE).by(__.values(Constants.VALUE).min());
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
		return null;
	}

	@Override
	public String[] getColumnHeaders() {
		return getHeaders();
	}
	
	private String[] getHeaders()
	{
		if(this.headerNames == null)
			headerNames =  (String[])(g.variables().get(Constants.HEADER_NAMES).get());
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
		return 0;
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
		// TODO Auto-generated method stub
		return null;
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
	public Iterator<List<Object[]>> standardizedUniqueIterator(
			String columnHeader, boolean getRawData) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData) {
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void refresh() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		List curValues = filterValues;
		if(filterHash.containsKey(columnHeader)) {
			curValues.addAll(filterHash.get(columnHeader));
		}
		filterHash.put(columnHeader, curValues);
	}

	@Override
	public void unfilter(String columnHeader) {
		filterHash.remove(columnHeader);
	}

//	@Override
//	public void unfilter(String columnHeader, List<Object> unfilterValues) {
//		// TODO Auto-generated method stub
//		List curValues = unfilterValues;
//		if(filterHash.containsKey(columnHeader))
//		{
//			curValues = filterHash.get(columnHeader);
//			curValues.removeAll(unfilterValues);
//		}
//		filterHash.put(columnHeader, curValues);		
//	}

	@Override
	public void unfilter() {
		filterHash.clear();		
	}

	@Override
	public void removeColumn(String columnHeader) {
		//this will be tricky depending on how we want to maintain the edgehash
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
		Vector retVector = null;
		
		// get all the levels
		getHeaders();
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = new GremlinBuilder(g);
		builder.setRange(startRange, endRange);
		
		
		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
			if(colIndex + 1 < headerNames.length)
				builder.addEdge(headerNames[colIndex], headerNames[colIndex+1]);
			else if(headerNames.length == 1)
			{
				builder.addNode(headerNames[0]);			
							
				
			}
			if(!columnsToSkip.contains(headerNames[colIndex])) {
				finalColumns.add(headerNames[colIndex]);
			}
		}
		
		// now add the projections
		builder.addSelector(finalColumns);
		
		// add the filters next
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			if(filterHash.containsKey(headerNames[colIndex]))
				builder.addFilter(headerNames[colIndex], filterHash.get(headerNames[colIndex]));
		}
		
		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript(g);
		
		if(gt.hasNext())
			retVector = new Vector();
		
		while(gt.hasNext())
		{
			Object data = gt.next();
			Object [] retObject = new Object[finalColumns.size()];
			
			//data will be a map for multi columns
			if(data instanceof Map) {
				for(int colIndex = 0;colIndex < finalColumns.size();colIndex++) {
					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
					retObject[colIndex] = ((Vertex)mapData.get(finalColumns.get(colIndex))).property(Constants.NAME).value();
				}
			} else {
				retObject[0] = ((Vertex)data).property(Constants.NAME).value();
			}
//			for(int colIndex = 0;colIndex < finalColumns.size();colIndex++)
//			{
////				retObject[colIndex] = ((Vertex)data.get(finalColumns.get(colIndex))).property(Constants.VALUE);
//				retObject[colIndex] = ((Vertex)data.get(finalColumns.get(colIndex))).property(Constants.VALUE).value();
////				retObject[colIndex] = ((Vertex)data).property(Constants.VALUE).value();
//			}
			retVector.add(retObject);
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

	@Override
	public List<Object[]> getRawData() {
		// the only important piece here is columns to skip
		// and then I am not sure if I need to worry about the filtered columns
		// create the return vector
		Vector retVector = null;
				
		// get all the levels
		getHeaders();
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = new GremlinBuilder(g);
		builder.setRange(startRange, endRange);
		
		//add edges if edges exist
		if(this.headerNames.length > 1) {
//			for(String node : edgeHash.keySet()) {
//				Set<String> edges = edgeHash.get(node);
//				for(String endNode : edges) {
//					builder.addEdge(node, endNode);
					HashMap<String, Set<String>> edgeMapCopy = new HashMap<String, Set<String>>();
					edgeMapCopy.putAll(this.edgeHash);
//					builder.addNodeEdge(headerNames[0], edgeMapCopy);
					builder.addNodeEdge(edgeMapCopy);
//				}
//			}
		} else {
			//no edges exist, add single node to builder
			builder.addNode(headerNames[0]);
		}
		
		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
//			if(colIndex + 1 < headerNames.length)
//				builder.addEdge(headerNames[colIndex], headerNames[colIndex+1]);
//			else if(headerNames.length == 1)
//			{
//				builder.addNode(headerNames[0]);
//			}
//			
			if(!columnsToSkip.contains(headerNames[colIndex])) {
				finalColumns.add(headerNames[colIndex]);
			}
		}
		
		// now add the projections
		builder.addSelector(finalColumns);
		
		// add the filters next
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			if(filterHash.containsKey(headerNames[colIndex]))
				builder.addFilter(headerNames[colIndex], filterHash.get(headerNames[colIndex]));
		}
		
		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript(g);
		
		if(gt.hasNext())
			retVector = new Vector();
		
		while(gt.hasNext())
		{
//			Map<String, Object> data = gt.next();
			Object data = gt.next();
			Object [] retObject = new Object[finalColumns.size()];
			
			//data will be a map for multi columns
			if(data instanceof Map) {
				for(int colIndex = 0;colIndex < finalColumns.size();colIndex++) {
					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
					retObject[colIndex] = ((Vertex)mapData.get(finalColumns.get(colIndex))).property(Constants.VALUE).value();
				}
			} else {
				retObject[0] = ((Vertex)data).property(Constants.VALUE).value();
			}

			retVector.add(retObject);
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
		getHeaders();
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = new GremlinBuilder(g);
		builder.setRange(startRange, endRange);
		
		
		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
			if(colIndex + 1 < headerNames.length)
				builder.addEdge(headerNames[colIndex], headerNames[colIndex+1]);
			else if(headerNames.length == 1)
				builder.addNode(headerNames[0]);			
			if(!columnsToSkip.contains(headerNames[colIndex]))
				finalColumns.add(headerNames[colIndex]);
		}
		
		// now add the projections
		builder.addSelector(finalColumns);
		
		// add the filters next
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			if(filterHash.containsKey(headerNames[colIndex]))
				builder.addFilter(headerNames[colIndex], filterHash.get(headerNames[colIndex]));
		}
		
		List<Object> itemToKeep = new ArrayList<>(1);
		itemToKeep.add(value);
		builder.addRestriction(columnHeader, itemToKeep);
		
		
		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript(g);
		
		
		if(gt.hasNext())
			retVector = new Vector();
		
		while(gt.hasNext())
		{
			Map<String, Object> data = gt.next();
			Object [] retObject = new Object[finalColumns.size()];
			for(int colIndex = 0;colIndex < finalColumns.size();colIndex++)
			{
				retObject[colIndex] = ((Vertex)data.get(finalColumns.get(colIndex))).property(Constants.VALUE);
			}
			retVector.add(retObject);
		}
		

		return retVector;
		
	}

	@Override
	public boolean isEmpty() {
		return g.traversal().V().hasNext();
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
		this.columnsToSkip.addAll(columnHeaders);
	}

	@Override
	public Object[] getFilteredUniqueRawValues(String columnHeader) {
		return null;
	}
	
	// create or add vertex
	private Vertex upsertVertex(String type, String data, Object value)
	{
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, type + ":" + data);
		if(gt.hasNext())
			retVertex = gt.next();
		else
			retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data);// push the actual value as well who knows when you would need it
			//retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data, Constants.FILTER, false); //should we add a filter flag to each vertex?
		return retVertex;
	}
	
	private Edge upsertEdge(Vertex fromVertex, Vertex toVertex)
	{
		Edge retEdge = null;
//		String edgeID = fromVertex.property(Constants.ID).value() + "" + toVertex.properties(Constants.ID);
		String edgeID = fromVertex.property(Constants.ID).value() + "" + toVertex.property(Constants.ID).value();
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(Constants.ID, edgeID);
		if(gt.hasNext())
		{
			retEdge = gt.next();
			Integer count = (Integer)retEdge.property(Constants.COUNT).value();
			count++;
			retEdge.property(Constants.COUNT, count);
		}
		else
		{
			retEdge = fromVertex.addEdge(edgeID, toVertex, Constants.ID, edgeID, Constants.COUNT, 1);
//			System.out.println("adding edge " + fromVertex.property(Constants.NAME).value() + "  to  " + toVertex.property(Constants.NAME).value());
		}

		return retEdge;
	}
	
	private void redoLevels(String [] newLevels)
	{
		if(this.headerNames == null){
			this.headerNames = newLevels;
			return;
		}
		// obviously miss the first one since that is not required
		String [] newLevelNames = new String[headerNames.length + newLevels.length - 1]; // minus one to remove the common piece
		System.arraycopy(headerNames, 0, newLevelNames, 0, headerNames.length); // copy previous array
		System.arraycopy(newLevels,1,newLevelNames,headerNames.length, newLevels.length - 1); // copied the new array

		g.variables().set(Constants.HEADER_NAMES, newLevelNames); // I dont know if i even need this moving forward.. but for now I will assume it is	
		
		headerNames = newLevelNames;
	}
	
	public void setRange(int startRange, int endRange)
	{
		this.startRange = startRange;
		this.endRange = endRange;
	}

	public Object[] getFilterModel() {
		// TODO Auto-generated method stub
		return new Integer[]{0, 1};
	}

	public Map<String, Object[]> getFilterTransformationValues() {
		// TODO Auto-generated method stub
		Map<String, Object[]> ftv = new HashMap<String, Object[]>();
		for(String key : this.filterHash.keySet()) {
			ftv.put(key, filterHash.get(key).toArray());
		}
		return ftv;
	}

	public String[] getFilteredColumns() {
		// TODO Auto-generated method stub
		return new String[0];
	}
	
	public Map<String, Set<String>> getEdgeHash() {
		return this.edgeHash;
	}
	
	public void setEdgeHash(Map<String, Set<String>> createEdgeHash) {
		this.edgeHash = createEdgeHash;
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
}
