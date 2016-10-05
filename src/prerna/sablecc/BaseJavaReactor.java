package prerna.sablecc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.DataFrameHelper;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.r.RSingleton;
import prerna.util.Console;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class BaseJavaReactor extends AbstractReactor{

	ITableDataFrame dataframe = null;
	PKQLRunner pkql = new PKQLRunner();
	boolean frameChanged = false;
	SecurityManager curManager = null;
	SecurityManager reactorManager = null;
	public static final String R_CONN = "R_CONN";
	public static final String R_PORT = "R_PORT";
	public RConnection rcon = null;
	
	public Console System = new Console();
	
	
	public BaseJavaReactor()
	{
		// empty constructor creates a frame
		dataframe = new H2Frame();
	}
	
	public void setCurSecurityManager(SecurityManager curManager)
	{
		this.curManager = curManager;
	}
	
	public void setReactorManager(SecurityManager reactorManager)
	{
		this.reactorManager = reactorManager;
	}
	
	
	public Connection getConnection()
	{
		return ((H2Frame)dataframe).getBuilder().getConnection();
	}
	
	public void setConsole()
	{
		this.System = new Console();
	}

	public BaseJavaReactor(ITableDataFrame frame)
	{
		// empty constructor creates a frame
		dataframe = frame;
	}
	
	// set a variable
	// use this variable if it is needed on the next call
	public void storeVariable(String varName, Object value)
	{
		pkql.setVariableValue(varName, value);
		
	}
	
	// set a variable
	// use this variable if it is needed on the next call
	public void removeVariable(String varName)
	{
		pkql.removeVariable(varName);
		
	}

	
	// get the variable back that was set
	public Object retrieveVariable(String varName)
	{
		return pkql.getVariableValue(varName);
	}
	
	// refresh the front end
	// use this call to indicate that you have manipulated the frame or have worked in terms of creating a newer frame
	public void refresh()
	{
		frameChanged = true;
		dataframe.updateDataId();
	}

	public void setPKQLRunner(PKQLRunner pkql)
	{
		this.pkql = pkql;
	}
	// couple of things I need here
	// access to the engines
	public IEngine getEngine(String engineName)
	{
		return null;
	}
	
	// sets the data frame
	public void setDataFrame(ITableDataFrame dataFrame)
	{
		this.dataframe = dataFrame;
	}
	
	public void runPKQL(String pkqlString)
	{
		// this will run PKQL
		System.out.println("Running pkql.. " + pkqlString);
		pkql.runPKQL(pkqlString, dataframe);
		
		myStore.put("RESPONSE", System.out.output);
	}
		
	public void preProcess()
	{
		
	}
	
	
	public void postProcess()
	{
		
	}

	public void filterNode(String columnHeader, String [] instances)
	{
		List <Object> values = new Vector();
		for(int instanceIndex = 0;instanceIndex < instances.length;values.add(instances[instanceIndex]), instanceIndex++);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, String value)
	{
		List <Object> values = new Vector();
		values.add(value);
		dataframe.filter(columnHeader, values);
	}

	public void filterNode(String columnHeader, List <Object> instances)
	{
		dataframe.filter(columnHeader, instances);		
	}

	public void runGremlin()
	{
		
	}
	
	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addReplacer(String pattern, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getValues2Sync(String childName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeReplacer(String pattern) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getValue(String key) {
		// TODO Auto-generated method stub
		// return the key for now
		return myStore.get(key);
	}

	@Override
	public void put(String key, Object value) {
		// TODO Auto-generated method stub
		myStore.put(key, value);
		
	}

	public void degree(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).degree(type, data);
			String output = "Degrees for  " + data + ":" + degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void removeNode(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			List<Object> removeList = new Vector<Object>();
			removeList.add(data);
			((TinkerFrame)dataframe).remove(type, removeList);
			String output = "Removed nodes for  " + data + " with values " + removeList;
			System.out.println(output);
			dataframe.updateDataId();
			removeNodeFromR(type, removeList);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void removeNodeFromR(String type, List nodeList)
	{
		RConnection rcon = startR();
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
		{
			
			String name = type + ":" + nodeList.get(nodeIndex);
			
			try{
				java.lang.System.out.println("Deleting.. " + name);
				rcon.eval("newGraph <- delete_vertices(" + graphName + ", V(" + graphName + ")[ID == \"" + name + "\"])");				
				rcon.eval(graphName + "<- newGraph");
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public void eigen(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).eigen(type, data);
			String output = "Eigen for  " + data + ":" +degree;
			System.out.println(output);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void isOrphan(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			boolean orphan = ((TinkerFrame)dataframe).isOrphan(type, data);
			String output = data + "  Orphan? " + orphan;
			System.out.println(output);
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	private String getBaseFolder()
	{
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// TODO Auto-generated catch block
			
		}
		if(baseFolder == null)
			baseFolder = "C:/users/pkapaleeswaran/workspacej3/SemossWeb";
		
		return baseFolder;

	}
	
	public RConnection startR()
	{
		RConnection retCon = (RConnection)retrieveVariable(R_CONN);
		java.lang.System.out.println("Connection right now is set to.. " + retCon);
		if(retCon == null) // <-- Trying to see if java works right here.. setting it to null and checking it!!
			try {
				RConnection masterCon = RSingleton.getConnection();
	
				String port = Utility.findOpenPort();
				
				java.lang.System.out.println("Starting it on port.. " + port);
				// need to find a way to get a common name
				masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
				retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
				storeVariable(R_CONN, retCon);
				storeVariable(R_PORT, port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		rcon = retCon;
		return retCon;
	}
	
	
	public String writeGraph(String directory)
	{
		String absoluteFileName = null;
		if(dataframe instanceof TinkerFrame)
		{
	    	final Graph graph = ((TinkerFrame)dataframe).g;
	    	absoluteFileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
	    	
	    	String fileName = directory + "/" + absoluteFileName; 
	    	try (final OutputStream os = new FileOutputStream(fileName)) {
	    	    graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
	    	}catch(Exception ex)
	    	{
	    		ex.printStackTrace();
	    	}
		}
		return absoluteFileName;
	}
	
	public void synchronizeToR(String graphName) // I will get the format later.. for now.. writing graphml
	{
		java.lang.System.setSecurityManager(curManager);
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		
		String wd = baseFolder + "/" + randomDir;
		java.io.File file = new File(wd);
		try {
			
			// create this directory
			file.mkdir();
			String fileName = writeGraph(wd);
			
			java.lang.System.out.println("Trying to get Connection.. ");
			RConnection rconn = startR();
			java.lang.System.out.println("Successful.. ");
			
			wd = wd.replace("\\", "/");
			
			// set the working directory
			rconn.eval("setwd(\"" + wd + "\")");
			
			// load the library
			rconn.eval("library(\"igraph\");");
			
			String loadGraphScript = graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			rconn.eval(loadGraphScript);
			
			System.out.println("successfully synchronized, your graph is now available as " + graphName);
			
			//rconn.close();
			storeVariable("GRAPH_NAME", graphName);	
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
		
	}
	
	private void getResultAsString(Object output, StringBuilder builder)
	{
//		try {
//			builder.append( ((REXP) output).asString() ) ;
//			return;
//		} catch (REXPMismatchException e) {
//			// do nothing
//		}
		
		// Generic vector..
		if(output instanceof REXPGenericVector) 
		{			
			RList list = ((REXPGenericVector)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPGenericVector)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// List..
		else if(output instanceof REXPList) {
			RList list = ((REXPList)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPList)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// Integers..
		else if(output instanceof REXPInteger)
		{
			int [] ints =  ((REXPInteger)output).asIntegers();
			if(ints.length > 1)
			{
				for(int intIndex = 0;intIndex < ints.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(ints[intIndex]);
					} else {
						builder.append(" ").append(ints[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(ints[0]);
			}
		}
		
		// Doubles.. 
		else if(output instanceof REXPDouble)
		{
			double [] doubles =  ((REXPDouble)output).asDoubles();
			if(doubles.length > 1)
			{
				for(int intIndex = 0;intIndex < doubles.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(doubles[intIndex]);
					} else {
						builder.append(" ").append(doubles[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(doubles[0]);
			}
		}
		
		// Strings..
		else if(output instanceof REXPString)
		{
			String [] strings =  ((REXPString)output).asStrings();
			if(strings.length > 1)
			{				
				for(int intIndex = 0;intIndex < strings.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(strings[intIndex]);
					} else {
						builder.append(" ").append(strings[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(strings[0]);
			}
		}
		
		builder.append("\n");
	}
	
	private String[] getAttributeArr(REXPList attrList) {
		if(attrList == null) {
			return null;
		}
		if(attrList.length() > 0) {	
			Object attr = attrList.asList().get(0);
			if(attr instanceof REXPString) {
				String[] strAttr = ((REXPString)attr).asStrings();
				return strAttr;
			}
		}
		return null;
	}
	
	public void runR(String script)
	{
		RConnection rcon = startR();
		try {
			Object output = rcon.eval(script);
			java.lang.System.out.println("RCon data.. " + output);
			StringBuilder builder = new StringBuilder();
			getResultAsString(output, builder);
			System.out.println("Output : " + builder.toString());
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// now is where the fun starts
	}

	public void clusterInfo()
	{
		String clusters = "clusters";
		clusterInfo(clusters);
	}
	
	public void clusterInfo(String clusterRoutine)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		
		RConnection con = startR();
		String clusters = "Component Information  \n";
		try
		{
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			con.eval("clus <- " + clusterRoutine + "(" + graphName +")");
			
			StringBuilder builder = new StringBuilder();
			
			Object output = con.eval("clus$no");
			getResultAsString(output, builder);
			clusters = clusters + " No. Of Components : " + builder.toString() + " \n";

			//reset the output string
			builder.setLength(0);

			clusters = clusters + " Component Sizes \n";
			output = con.eval("clus$csize");
			getResultAsString(output, builder);
			clusters = clusters + builder.toString();
			
			System.out.println(clusters);
			colorClusters();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	
	public void walkInfo()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		
		RConnection con = startR();
		String clusters = "Component Information  \n";
		try
		{
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			con.eval("clus <- cluster_walktrap(" + graphName +", membership=TRUE)");
			/*Object output = con.eval("clus$no");
			clusters = clusters + " No. Of Components : " + getResultAsString(output) + " \n";
			output = con.eval("clus$csize");
			clusters = clusters + " Component Sizes \n";
			clusters = clusters + getResultAsString(output);*/
			clusters = clusters + "Completed Walktrap";
			System.out.println(clusters);
			colorClusters();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	// remove the node on R
	// get the number of clustered components
	// perform a layout
	// color a graph based on a formula
	
	public void key()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		String names = "";
		RConnection con = startR();
		try {
			// get the articulation points
			int [] vertices = con.eval("articulation.points(" + graphName + ")").asIntegers();
			// now for each vertex get the name
			Hashtable <String, String> dataHash = new Hashtable<String, String>();
			for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++)
			{
				String output = con.eval("vertex_attr(" + graphName + ", \"ID\", " + vertices[vertIndex] + ")").asString();
				String [] typeData = output.split(":");
				String typeOutput = "";
				if(dataHash.containsKey(typeData[0]))
					typeOutput = dataHash.get(typeData[0]);
				typeOutput = typeOutput + "  " + typeData[1];
				dataHash.put(typeData[0], typeOutput);
			}
			
			Enumeration <String> keys = dataHash.keys();
			while(keys.hasMoreElements())
			{
				String thisKey = keys.nextElement();
				names = names + thisKey + " : " + dataHash.get(thisKey) + "\n";
			}
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(" Key Nodes \n " + names);
	}
	
	public void colorClusters(String clusterName)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		RConnection con = startR();
		// the color is saved as color
		try
		{
			int [] memberships = rcon.eval(clusterName + "$membership").asIntegers();
			String [] IDs = rcon.eval("V(" + graphName + ")$ID").asStrings();
			
			for(int memIndex = 0;memIndex < memberships.length;memIndex++)
			{
				String thisID = IDs[memIndex];

				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(Constants.ID, thisID);
				if(gt.hasNext()) {
					retVertex = gt.next();
				}
				if(retVertex != null)
				{
					retVertex.property("CLUSTER", memberships[memIndex]);
					java.lang.System.out.println("Set the cluster to " + memberships[memIndex]);
				}
			}
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void colorClusters()
	{
		String clusterName = (String)retrieveVariable("CLUSTER_NAME");
		colorClusters(clusterName);
	}
	
	public void doLayout(String layout)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		RConnection con = startR();
		// the color is saved as color
		try
		{
			con.eval("xy_layout <- " + layout + "(" + graphName +")");
			synchronizeXY("xy_layout");
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void synchronizeXY(String rVariable)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		RConnection con = startR();
		try
		{
			double [][] memberships = rcon.eval("xy_layout").asDoubleMatrix();
			String [] axis = null;
			if(memberships[0].length == 2) {
				axis = new String[]{"X", "Y"};
			} else if(memberships[0].length == 3) {
				axis = new String[]{"X", "Y", "Z"};
			}
			
			String [] IDs = rcon.eval("V(" + graphName + ")$ID").asStrings();
			
			for(int memIndex = 0; memIndex < memberships.length; memIndex++)
			{
				String thisID = IDs[memIndex];
	
				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(Constants.ID, thisID);
				if(gt.hasNext()) {
					retVertex = gt.next();
				}
				if(retVertex != null)
				{
					for(int i = 0; i < axis.length; i++) {
						retVertex.property(axis[i], memberships[memIndex][i]);
					}
					java.lang.System.out.println("Set the cluster to " + memberships[memIndex]);
				}
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
		
	public void synchronizeFromR(String graphName)
	{
		// get the attributes
		// and then synchronize all the different properties
		// vertex_attr_names
		String names = "";
		RConnection con = startR();

		// get all the attributes first
		try {
			String [] strings = con.eval("vertex_attr_names(" + graphName + ")").asStrings();
			// the question is do I get everything here and set tinker
			// or for each get it and so I dont look up tinker many times ?!
			
			// now I need to get each of this string and then synchronize
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public void synchronizeFromR()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		synchronizeFromR(graphName);
	}
	
	
	
	public void generateNewGraph(String[] selectors, Map<String, String> edges) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			TinkerFrame newDataFrame = DataFrameHelper.generateNewGraph((TinkerFrame) dataframe, selectors, edges);
			myStore.put("G", newDataFrame);
			System.out.println("Generated new graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
		
	}
	
	public void endR()
	{
		java.lang.System.setSecurityManager(curManager);
		RConnection con = startR();
		try {
			con.shutdown();
//			System.out.println("R Shutdown!!");
//			java.lang.System.setSecurityManager(reactorManager);
/*		String port = (String)retrieveVariable("R_PORT");
		System.out.println("Trying to connect to port.. " + port);
		runR("library(RSclient);");
		runR("library(Rserve);");
		runR("rsc <- RSconnect(host=\"127.0.0.1\", port = "+ port + ");");
		runR("RSshutdown(rsc);");
		runR("RSclose();");
*/		
		// clean up other things
		removeVariable(R_CONN);
		removeVariable(R_PORT);
		System.out.println("R Shutdown!!");
		java.lang.System.setSecurityManager(reactorManager);
		} catch (Exception e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	}
	
	public void runClustering(int instanceIndex, int numClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MATH_PARAM, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runLOF(int instanceIndex, int k, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.LOFReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.LOFReactor.K_NEIGHBORS.toUpperCase(), k);
		
		prerna.algorithm.impl.LOFReactor alg = new prerna.algorithm.impl.LOFReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MATH_PARAM, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runOutlier(int instanceIndex, int numSubsetSize, int numRums, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.OutlierReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.OutlierReactor.NUM_SAMPLE_SIZE.toUpperCase(), numSubsetSize);
		params.put(prerna.algorithm.impl.OutlierReactor.NUMBER_OF_RUNS.toUpperCase(), numRums);
		
		prerna.algorithm.impl.OutlierReactor alg = new prerna.algorithm.impl.OutlierReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MATH_PARAM, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runSimilarity(int instanceIndex, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.SimilarityReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		
		prerna.algorithm.impl.SimilarityReactor alg = new prerna.algorithm.impl.SimilarityReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MATH_PARAM, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
}
