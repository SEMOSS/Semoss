package prerna.sablecc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.DataFrameHelper;
import prerna.ds.TinkerFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.r.RSingleton;
import prerna.util.Console;
import prerna.util.DIHelper;
import prerna.util.Utility;


public abstract class BaseJavaReactor extends AbstractReactor{

	ITableDataFrame dataframe = null;
	PKQLRunner pkql = new PKQLRunner();
	boolean frameChanged = false;
	SecurityManager curManager = null;
	SecurityManager reactorManager = null;
	public static final String R_CONN = "R_CONN";
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
		RConnection rcon = getR();
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
	
	private RConnection getR()
	{
		RConnection retCon = (RConnection)retrieveVariable(R_CONN);
		java.lang.System.out.println("Connection right now is set to.. " + retCon);
		if(retCon == null)
			try {
				RConnection masterCon = RSingleton.getConnection();
	
				String port = Utility.findOpenPort();
				
				java.lang.System.out.println("Starting it on port.. " + port);
				// need to find a way to get a common name
				//masterCon.eval("Rserve(" + port + ")");
				retCon = masterCon; //new RConnection("localhost", Integer.parseInt(port));
				storeVariable(R_CONN, retCon);
				storeVariable("R_PORT", port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		rcon = retCon;
		return retCon;
	}
	
	public void closeR()
	{
		String port = (String)retrieveVariable("R_PORT");
		RConnection conn2 = (RConnection)retrieveVariable(R_CONN);
		try {
			conn2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String writeGraph(String directory)
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
			RConnection rconn = getR();
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
	
	private String getResultAsString(Object output)
	{
		String intString = "";
		if(output instanceof REXPInteger)
		{
			int [] ints =  ((REXPInteger)output).asIntegers();
			if(ints.length > 1)
			{
				
				for(int intIndex = 0;intIndex < ints.length;intString = intString + " " + ints[intIndex], intIndex++);
					
			}
			else
			{					
				intString = ints[0] + "";
			}
		}
		
		// Doubles.. 
		if(output instanceof REXPDouble)
		{
			double [] doubles =  ((REXPDouble)output).asDoubles();
			if(doubles.length > 1)
			{
				
				for(int intIndex = 0;intIndex < doubles.length;intString = intString + " " + doubles[intIndex], intIndex++);
			}
			else
			{					
				intString = doubles[0] + "";
			}
		}
		// strings
		if(output instanceof REXPString)
		{
			String [] ints =  ((REXPString)output).asStrings();
			if(ints.length > 1)
			{				
				for(int intIndex = 0;intIndex < ints.length;intString = intString + " " + ints[intIndex], intIndex++);
					
			}
			else
			{					
				intString = ints[0] + "";
			}
		}
		
		return intString;
	}
	
	public void runR(String script)
	{
		RConnection rcon = getR();
		try {
			Object output = rcon.eval(script);
			java.lang.System.out.println("RCon data.. " + output);
			System.out.println("Output : " + getResultAsString(output));
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// now is where the fun starts
	}
	
	public void clusterInfo()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		RConnection con = getR();
		String clusters = "Component Information  \n";
		try
		{
			// set the clusters
			con.eval("clus <- clusters(" + graphName +")");
			Object output = con.eval("clus$no");
			clusters = clusters + " No. Of Components : " + getResultAsString(output) + " \n";
			output = con.eval("clus$csize");
			clusters = clusters + " Component Sizes \n";
			clusters = clusters + getResultAsString(output);
			System.out.println(clusters);
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
		RConnection con = getR();
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
	
	public void synchronizeFromR(String graphName)
	{
		// get the attributes
		// and then synchronize all the different properties
		// vertex_attr_names

		
		
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
}
