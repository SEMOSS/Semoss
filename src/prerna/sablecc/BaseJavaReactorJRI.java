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
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.DataFrameHelper;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.r.RSingleton;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Console;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class BaseJavaReactorJRI extends BaseJavaReactor{

	ITableDataFrame dataframe = null;
	PKQLRunner pkql = new PKQLRunner();
	boolean frameChanged = false;
	SecurityManager curManager = null;
	SecurityManager reactorManager = null;
	public static final String R_CONN2 = "R_CONN";
	public static final String R_ENGINE = "R_ENGINE";
	public static final String R_PORT = "R_PORT";
	public RConnection rcon = null;
	String wd = null;
	String fileName = null;
	
	public Console System = new Console();
	
	
	public BaseJavaReactorJRI()
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

	public BaseJavaReactorJRI(ITableDataFrame frame)
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
		Rengine rengine = (Rengine)retrieveVariable(R_ENGINE);
		
		// make sure current connection exists
		if(rcon != null) {
			String graphName = (String)retrieveVariable("GRAPH_NAME");
			for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++)
			{
				String name = type + ":" + nodeList.get(nodeIndex);
				try{
					java.lang.System.out.println("Deleting.. " + name);
					rengine.eval(graphName + " <- delete_vertices(" + graphName + ", V(" + graphName + ")[vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\") == \"" + name + "\"])");				
				}catch(Exception ex)
				{
					ex.printStackTrace();
				}
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
	
	public void reconnectR(int port)
	{
		RSingleton.getConnection(port);
	}
	
	public Object startR()
	{
		
		Rengine retEngine = Rengine.getMainEngine();
		java.lang.System.out.println("Connection right now is set to.. " + retEngine);
		// i need to find if this is rserve or JRI
		
		if(retEngine == null) // <-- Trying to see if java works right here.. setting it to null and checking it!!
		{
			try {
				
				// start the R Engine
				String args1[] = new String[10];
				retEngine = new Rengine(args1, false, null);
				System.out.println("Created Engine.. ");
				//retEngine = new Rengine();
				
				// load all the libraries
				retEngine.eval("library(splitstackshape);");
				// data table
				retEngine.eval("library(data.table);");
				// reshape2
				retEngine.eval("library(reshape2);");
				// rjdbbc
				retEngine.eval("library(RJDBC);");

				// not sure if I need dplyr too I will get to it
				storeVariable(R_ENGINE, retEngine);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retEngine;
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
	
	private void synchronizeGraphToR(String graphName, String wd)
	{
		java.io.File file = new File(wd);
		try {
			
			// create this directory
			file.mkdir();
			fileName = writeGraph(wd);
			
			java.lang.System.out.println("Trying to get Connection.. ");
			Rengine retEngine = (Rengine)startR();
			java.lang.System.out.println("Successful.. ");
			
			wd = wd.replace("\\", "/");
			
			// set the working directory
			retEngine.eval("setwd(\"" + wd + "\")");
			
			// load the library
			retEngine.eval("library(\"igraph\");");
			
			String loadGraphScript = graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			retEngine.eval(loadGraphScript);
			
			System.out.println("successfully synchronized, your graph is now available as " + graphName);
			
			//rconn.close();
			storeVariable("GRAPH_NAME", graphName);	
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
		
	}
	
	
	public void synchronizeToR(String graphName) // I will get the format later.. for now.. writing graphml
	{
		java.lang.System.setSecurityManager(curManager);
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		wd = baseFolder + "/" + randomDir;		
		if(dataframe instanceof TinkerFrame)
			synchronizeGraphToR(graphName, wd);
		else if(dataframe instanceof H2Frame)
			synchronizeGridToR(graphName);
	}
	
	private void initiateDriver(String url, String username)
	{
		String driver = "org.h2.Driver";
		String jarLocation = "";
		if(retrieveVariable("H2DRIVER_PATH") == null)
		{
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");;
			String library = "RJDBC";
			String jar = "h2-1.4.185.jar"; // TODO: create an enum of available drivers and the necessary jar for each
			jarLocation = workingDir + "/RDFGraphLib/" + jar;
		// line of R that loads database driver and jar
		}
		else
			jarLocation = (String)retrieveVariable("H2DRIVER_PATH");
		java.lang.System.out.println("Loading driver.. " + jarLocation);
		String script = "drv <- JDBC('" + driver + "', '" + jarLocation  + "', identifier.quote='`');" 
			+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', '')"; // line of R script that connects to H2Frame
		runR(script);
	}
	
	public void setH2Driver(String directory)
	{
		storeVariable("H2DRIVER_PATH", directory);
	}
	
	public void synchronizeGridToR(String frameName)
	{
		synchronizeGridToR(frameName, null);
	}
	
	private void synchronizeGridToR(String frameName, String cols)
	{
		H2Frame gridFrame = (H2Frame)dataframe;
		String tableName = gridFrame.getBuilder().getTableName();
		String url = gridFrame.getBuilder().connectFrame();
		url = url.replace("\\", "/");
		initiateDriver(url, "sa");
		
		String selectors = "*";
		if(cols != null && cols.length() >= 0)
		{
			selectors = "";
			String [] colSelectors = cols.split(";");
			for(int selectIndex = 0;selectIndex < colSelectors.length;selectIndex++)
			{
				selectors = selectors + colSelectors[selectIndex];
				if(selectIndex + 1 < colSelectors.length)
					selectors = selectors + ", ";
			}
			
		}
		// make a selector based on col csv now
		//runR(frameName + " <-as.data.table(unclass(dbReadTable(conn,'" + tableName + "')));", false);
		runR(frameName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName + "')));", false);
		// should be dbGetQuery(conn, "Select colNames.. ")
		runR("setDT(" + frameName + ")", false);

		storeVariable("GRID_NAME", frameName);	
		
		System.out.println("Completed synchronization as " + frameName);
	}
	
	public void synchronizeGridFromR(String frameName, String tableName)
	{
		// assumes this is a grid and tries to write back the table
		runR("dbWriteTable(conn,'" + tableName +"', " + frameName + ");", false);
		System.out.println("Output is now available as " + tableName);
	}

	public void synchronizeGridFromR(String frameName, String tableName, String cols)
	{
		// assumes this is a grid and tries to write back the table
		// I need to make another data table with these specific columns and then write that
		startR();
		try {
			if(cols != null && cols.length() > 0)
			{
				String script = ".(";
				String [] reqCols = cols.split(";");
				// I need to take just these columns and then synchronize this back
				for(int colIndex = 0;colIndex < reqCols.length;colIndex++)
				{
					script = script + reqCols[colIndex].toUpperCase();
					if(colIndex + 1 < reqCols.length)
						script = script + ", ";
				}
				script = script + ")";
				String tempName = Utility.getRandomString(8);
				
				String finalScript = tempName + " <- " + frameName + "[, " + script + "]; dbWriteTable(conn,'" + tableName +"', " + tempName + ");";
				rcon.eval(finalScript);		
			}
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//TBD
		//runR("dbWriteTable(conn,'" + tableName +"', " + frameName + ");", false);
		//System.out.println("Output is now available as " + tableName);
	}

	public void filterValues(String frameName, String columnName, String values)
	{
		
	}
	
	public void synchronizeGridFromR(String frameName, boolean self)
	{
		// assumes this is a grid and tries to write back the table
		H2Frame gridFrame = (H2Frame)dataframe;
		String tableName = gridFrame.getBuilder().getTableName();
		if(!self)
			tableName = Utility.getRandomString(8);
		runR("dbWriteTable(conn,'" + tableName +"', " + frameName + ");", false);
		
		System.out.println("Table Synchronized as " + tableName);
	}
	
	public void synchronizeGridFromR()
	{
		String frameName = (String)retrieveVariable("GRID_NAME");
		synchronizeGridFromR(frameName, false);
	}

	public void synchronizeCSVToR(String fileName, String frameName)
	{
		//runR(frameName + " <-as.data.table(unclass(dbReadTable(conn,'" + tableName + "')));");		
		String javaFileName = fileName.replace("\\", "/");
		// lapply(lapply(dfs,get),function(x) {colnames(x) <- tolower(colnames(x));x})
		String script = frameName + " <- fread(\"" + javaFileName + "\")"
				+ "lapply(lapply(" + frameName + " , get), function(x) {colnames(x) <- toupper(colnames(x));x}" // change all to upper case
				+ ""; // replace space with underscore
		
		
		runR(frameName + " <- fread(\"" + javaFileName + "\")", false);
		System.out.println("Completed synchronization of CSV " + fileName);
	}
	
	
	private void getResultAsString(Object output, StringBuilder builder)
	{
		// Generic vector..
		if(output instanceof REXPGenericVector) 
		{			
			org.rosuda.REngine.RList list = ((REXPGenericVector)output).asList();
			
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
			org.rosuda.REngine.RList list = ((REXPList)output).asList();
			
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
		runR(script, true);
	}
	
	public void runR(String script, boolean result)
	{
		Rengine retEngine = (Rengine)startR();
		String newScript = "paste(capture.output(print(" + script + ")),collapse='\n')";
		try {
			REXP output = retEngine.eval(newScript);
			if(result) {
				System.out.println(output.asString());
			}
		} catch(Exception e) {
			try {
				Object output = rcon.eval(script);
				if(result)
				{
					java.lang.System.out.println("RCon data.. " + output);
					StringBuilder builder = new StringBuilder();
					getResultAsString(output, builder);
					System.out.println("Output : " + builder.toString());
				}
			} catch (REngineException e2) {
				e2.printStackTrace();
				String errorMessage = null;
				if(e2.getMessage() != null && !e2.getMessage().isEmpty()) {
					errorMessage = e2.getMessage();
				} else {
					errorMessage = "Unexpected error in execution of R routine ::: " + script;
				}
				throw new IllegalArgumentException(errorMessage);
			}
		}
//		try {
//			Object output = rcon.eval(script);
//			if(result)
//			{
//				java.lang.System.out.println("RCon data.. " + output);
//				StringBuilder builder = new StringBuilder();
//				getResultAsString(output, builder);
//				System.out.println("Output : " + builder.toString());
//			}
//		} catch (RserveException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.err.println("Errored.. ");
//		}
		// now is where the fun starts
	}

	public void clusterInfo()
	{
		String clusters = "clusters";
		clusterInfo(clusters);
	}
	
	// replace the column value for a particular column
	public void replaceColumnValue(String frameName, String columnName, String curValue, String newValue)
	{
		// * dt[PY == "hello", PY := "D"] replaces a column conditionally based on the value
		startR();
		// need to get the type of this
		try {
			String condition = " ,";
			columnName = columnName.toUpperCase();

			String output = rcon.eval("sapply(" + frameName + "$" + columnName.toUpperCase() + ", class);").asString();
			String quote = "";
			if(output.contains("character"))
				quote = "\"";
			if(curValue != null)
				condition = columnName + " == " + quote + curValue + quote + ", ";
			String script = frameName + "[" + condition + columnName + " := " + quote + newValue + quote + "]";
			rcon.eval(script);
			System.out.println("Complete ");
		} catch (RserveException | REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void splitColumn(String frameName, String columnName, String separator)
	{
		splitColumn(frameName, columnName, separator, false, true);
	}
		
	// split a column based on a value
	public void splitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace)
	{
		//  cSplit(dt, "PREFIX", "_")
		startR();
		// need to get the type of this
		try {
			String tempName = Utility.getRandomString(8);
			
			String frameReplaceScript = frameName + " <- " + tempName + ";";
			if(!frameReplace)
				frameReplaceScript = "";
			String columnReplaceScript = "TRUE";
			if(!dropColumn)
				columnReplaceScript = "FALSE";
			String script = tempName + " <- cSplit(" + frameName + ", \"" + columnName.toUpperCase() + "\", \"" + separator + "\", drop = " + columnReplaceScript+ ");" 
				//+ tempName +" <- " + tempName + "[,lapply(.SD, as.character)];"  // this ends up converting numeric to factors too
				//+ frameReplaceScript
				;
			rcon.eval(script);
			System.out.println("Script " + script);
			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			String [] factors = rcon.eval(script).asStrings();			
			String [] colNames = getColNames(tempName);
			
			// now I need to compose a string based on it
			String conversionString = "";
			for(int factorIndex = 0;factorIndex < factors.length;factorIndex++)
			{
				if(factors[factorIndex].equalsIgnoreCase("TRUE")) // this is a factor
				{
					conversionString = conversionString + 
							tempName + "$" + colNames[factorIndex] + " <- "
							+ "as.character(" + tempName + "$" +colNames[factorIndex] + ");";
				}
			}
			rcon.eval(conversionString + frameReplaceScript);
			
			System.out.println("Script " + script);
			System.out.println("Complete ");
			// once this is done.. I need to find what the original type is and then apply a type to it
			// may be as string
			// or as numeric
			// else this is not giving me what I want :(
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	// change column name
	public void changeName(String oldName, String newName)
	{
		// this will be done through h2 piece ?
		
	}
	
	
	public String[] getColNames(String frameName, boolean print)
	{
		startR();
		String [] colNames = null;
		try {
				String script = "matrix(colnames(" + frameName + "));";
				colNames = rcon.eval(script).asStrings();
				if(print)
				{
					System.out.println("Columns..");
					for(int colIndex = 0;colIndex < colNames.length;colIndex++)
						System.out.println(colNames[colIndex] + "\n");
				}
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return colNames;
	}
	
	public String[] getColNames(String frameName)
	{
		return getColNames(frameName, true);
	}
	

	public String[] getColTypes(String frameName, boolean print)
	{
		startR();
		String [] colTypes = null;
		try {
				String script = "matrix(sapply(" + frameName + ", class));";
				colTypes = rcon.eval(script).asStrings();
				if(print)
				{
					System.out.println("Columns..");
					for(int colIndex = 0;colIndex < colTypes.length;colIndex++)
						System.out.println(colTypes[colIndex] + "\n");
				}
				
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return colTypes;
	}

	// gives the different types of columns that are there and how many are there of that type
	// such as 5 integer columns
	// 3 string columns
	// 4 blank columns etc. 
	public void getColumnTypeCount(String frameName)
	{
		startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String script = "";
			
			String [] colNames = getColNames(frameName, false);
			// I am not sure if I need the colnames right now but...
			
			// get the column types
			String [] colTypes = getColTypes(frameName, false);
			
			// get the blank columns
			script = "matrix( " + frameName + "[, colSums( " + frameName + " != \"\") !=0])";
			String [] blankCols = rcon.eval(script).asStrings();
			
			Hashtable <String, Integer> colCount = new Hashtable <String, Integer>();
			
			for(int colIndex = 0;colIndex < colTypes.length;colIndex++)
			{
				String colType = colTypes[colIndex];
				if(blankCols[colIndex].equalsIgnoreCase("FALSE"))
					colType = "Empty";
				int count = 0;
				if(colCount.containsKey(colType))
					count = colCount.get(colType);
				
				count++;
				colCount.put(colType, count);
			}
			
			StringBuilder builder = new StringBuilder();
			builder.append(colCount + "");
			System.out.println("Output : " + builder.toString());
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	public void getColumnCount(String column)
	{
		startR();
		String frame = (String)retrieveVariable("GRID_NAME");
		getColumnCount(frame, column);
	}

	
	public void joinColumns(String frameName, String newColumnName,  String separator, String cols)
	{
		// reconstruct the column names
		//paste(df1$a_1, df1$a_2, sep="$$")
		try {
			startR();
			String [] columns = cols.split(";");
			String concatString = "paste(";
			for(int colIndex = 0;colIndex < columns.length;colIndex++)
			{
				concatString = concatString + frameName + "$" + columns[colIndex].toUpperCase();
				if(colIndex + 1 < columns.length)
					concatString = concatString + ", ";
			}
			concatString = concatString + ", sep= \"" + separator + "\")";
			
			String script = frameName + "$" + newColumnName.toUpperCase() + " <- " + concatString;
			System.out.println(script);
			rcon.eval(script);
			System.out.println("Join Complete ");
		
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Object[][] getColumnCount(String frameName, String column)
	{
		// get all the column names first
		/*
		 * colnames(frame) <-- this can probably be skipped because we have the metadata
		 * class(frame$name)
		 * frame[, .N, by="column"] <-- gives me all the counts of various values
		 * matrix(sapply(DT, class), byrow=TRUE) <-- makes it into a full array so I can ick through the types
		 * strsplit(x,".", fixed=T) splits the string based on a delimiter.. fixed says is it based on regex or not 
		 * dt[PY == "hello", PY := "D"] replaces a column conditionally based on the value
		 * dt[, val4:=""] <-- add a new column called val 4
		 * matrix(dt[, colSums(dt != "") !=0]) <-- gives me if a column is blank or not.. 
		 * 
		 */
		// start the R first
		startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			
			String script = "colData <-  " + frameName + "[, .N, by=\"" + column.toUpperCase() +"\"];";
			System.out.println("Script is " + script);
			rcon.eval(script);
			
			script = "colData$" + column.toUpperCase();
			String [] uniqueColumns = rcon.eval(script).asStrings();
			// need to limit this eventually to may be 10-15 and no more
			script = "matrix(colData$N);"; 
			int [] colCount = rcon.eval(script).asIntegers();
			retOutput = new Object[uniqueColumns.length][2];
			StringBuilder builder = new StringBuilder();
			builder.append(column + "\t Count \n");
			for(int outputIndex = 0;outputIndex < uniqueColumns.length;outputIndex++)
			{
				retOutput[outputIndex][0] = uniqueColumns[outputIndex];
				retOutput[outputIndex][1] = colCount[outputIndex];
				builder.append(retOutput[outputIndex][0] + "\t" + retOutput[outputIndex][1] + "\n");
			}
			System.out.println("Output : " + builder.toString());
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retOutput;
	}
	
	public void unpivot()
	{
		String frameName = (String)retrieveVariable("GRID_NAME");
		unpivot(frameName, null, true);
	}
	
	public void cleanColumns()
	{
		//colnames(movies)[colnames(movies)=="Hello 1"] <- sub(" ", "_", "Hello 1") // moves the spaces to underscores
	}
	
	public void unpivot(String frameName, String cols, boolean replace)
	{
		// makes the columns and converts them into rows
		// need someway to indicate to our metadata as well that this is gone
		//melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		
		String concatString = "";
		startR();
		String tempName = Utility.getRandomString(8);
		
		if(cols != null && cols.length() > 0)
		{
			String [] columnsToPivot = cols.split(";");
			concatString = ", measure.vars = c(";
			for(int colIndex = 0;colIndex < columnsToPivot.length;colIndex++)
			{
				concatString = concatString + "\"" + columnsToPivot[colIndex] + "\"";
				if(colIndex + 1 < columnsToPivot.length)
					concatString = concatString + ", ";
			}
			concatString = concatString + ")";
		}
		String replacer = "";
		if(replace)
			replacer = frameName + " <- " + tempName;
		String script = tempName + "<- melt(" + frameName + concatString + ");" 
						+ tempName + " <- " + tempName + "[,lapply(.SD, as.character)];"
						+ replacer;
		System.out.println("executing script " + script);
		try {
			rcon.eval(script);
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void pivot(String columnToPivot, String cols)
	{
		String frameName = (String)retrieveVariable("GRID_NAME");
		pivot(frameName, true, columnToPivot, cols);
	}

	public void pivot(String frameName, boolean replace,String columnToPivot, String cols)
	{
		// makes the columns and converts them into rows
		
		//dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		
		startR();
		String newFrame = Utility.getRandomString(8);
		
		String replaceString = "";
		if(replace)
			replaceString = frameName + " <- " + newFrame;
		
		String keepString = ""; 
		if(cols != null && cols.length() > 0)
		{
			String [] columnsToKeep = cols.split(";");
			keepString = ", formula = ";
			for(int colIndex = 0;colIndex < columnsToKeep.length;colIndex++)
			{
				keepString = keepString + "\"" + columnsToKeep[colIndex] + "\"";
				if(colIndex + 1 < columnsToKeep.length)
					keepString = keepString + " + ";
			}
			keepString = keepString + " ~ " + columnToPivot;
		}
		
		String script = newFrame + " <- dcast(" + frameName + keepString + ");"
						+ replaceString ;
		System.out.println("executing script " + script);
		try {
			rcon.eval(script);
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void sampleR(String frameName)
	{
		//> DT[,i := .I][sample(i,3)] <-- number of samples here is 3
	}
	
	public void clusterInfo(String clusterRoutine)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		
		Rengine retEngine = (Rengine)startR();
//		String clusters = "Component Information  \n";
		try
		{
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			retEngine.eval("clus <- " + clusterRoutine + "(" + graphName +")");
			
//			StringBuilder builder = new StringBuilder();
			
			System.out.println("\n No. Of Components :");
			runR("clus$no");
//			Object output = con.eval("clus$no");
//			getResultAsString(output, builder);
//			clusters = clusters + " No. Of Components : " + builder.toString() + " \n";

			//reset the output string
//			builder.setLength(0);

			System.out.println("\n Component Sizes :");
			runR("clus$csize");

//			clusters = clusters + " Component Sizes \n";
//			output = con.eval("clus$csize");
//			getResultAsString(output, builder);
//			clusters = clusters + builder.toString();
			
//			System.out.println(clusters);
			colorClusters();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	
	public void walkInfo()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		
		Rengine retEngine = (Rengine)startR();
		String clusters = "Component Information  \n";
		try
		{
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			retEngine.eval("clus <- cluster_walktrap(" + graphName +", membership=TRUE)");
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
		Rengine retEngine = (Rengine)startR();
		try {
			// get the articulation points
			int [] vertices = retEngine.eval("articulation.points(" + graphName + ")").asIntArray();
			// now for each vertex get the name
			Hashtable <String, String> dataHash = new Hashtable<String, String>();
			for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++)
			{
				String output = retEngine.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\", " + vertices[vertIndex] + ")").asString();
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		System.out.println(" Key Nodes \n " + names);
	}
	
	public void colorClusters(String clusterName)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		Rengine retEngine = (Rengine)startR();
		// the color is saved as color
		try
		{
			int [] memberships = rcon.eval(clusterName + "$membership").asIntegers();
			String [] IDs = rcon.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStrings();
			
			for(int memIndex = 0;memIndex < memberships.length;memIndex++)
			{
				String thisID = IDs[memIndex];

				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
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

		Rengine retEngine = (Rengine)startR();
		// the color is saved as color
		try
		{
			retEngine.eval("xy_layout <- " + layout + "(" + graphName +")");
			synchronizeXY("xy_layout");
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void synchronizeXY(String rVariable)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		Rengine retEngine = (Rengine)startR();
		try
		{
			double [][] memberships = rcon.eval("xy_layout").asDoubleMatrix();
			String [] axis = null;
			if(memberships[0].length == 2) {
				axis = new String[]{"X", "Y"};
			} else if(memberships[0].length == 3) {
				axis = new String[]{"X", "Y", "Z"};
			}
			
			String [] IDs = rcon.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStrings();
			
			for(int memIndex = 0; memIndex < memberships.length; memIndex++)
			{
				String thisID = IDs[memIndex];
	
				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
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
		Rengine retEngine = (Rengine)startR();

		// get all the attributes first
		try {
			String [] strings = retEngine.eval("vertex_attr_names(" + graphName + ")").asStringArray();
			// the question is do I get everything here and set tinker
			// or for each get it and so I dont look up tinker many times ?!
			
			// now I need to get each of this string and then synchronize
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	// need to figure this out
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
		Rengine retCon = (Rengine)retrieveVariable(R_ENGINE);
		try {
			if(retCon != null)
				retCon.end();;
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
		removeVariable(R_ENGINE);
		removeVariable(R_PORT);
		System.out.println("R Shutdown!!");
		java.lang.System.setSecurityManager(reactorManager);
		} catch (Exception e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	}
	
	public void cleanup()
	{
		// introducing this method to clean up everything 
		// remove all the stored variables
		
		
		// clean up R connection
		endR();
		// cleanup the directory
		java.lang.System.setSecurityManager(curManager);
		File file = new File(fileName);
		file.delete();
		file = new File(wd);
		file.delete();
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void initR(int port)
	{
		RSingleton.getConnection(port);
	}
	
	public void runClustering(int instanceIndex, int numClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runClustering(String columnName, int numClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runMultiClustering(int instanceIndex, int minNumClusters, int maxNumClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.MultiClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MIN_NUM_CLUSTERS.toUpperCase(), minNumClusters);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MAX_NUM_CLUSTERS.toUpperCase(), maxNumClusters);

		prerna.algorithm.impl.MultiClusteringReactor alg = new prerna.algorithm.impl.MultiClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runMultiClustering(String columnName, int minNumClusters, int maxNumClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.MultiClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MIN_NUM_CLUSTERS.toUpperCase(), minNumClusters);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MAX_NUM_CLUSTERS.toUpperCase(), maxNumClusters);

		prerna.algorithm.impl.MultiClusteringReactor alg = new prerna.algorithm.impl.MultiClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
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
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runLOF(String columnName, int k, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.LOFReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.LOFReactor.K_NEIGHBORS.toUpperCase(), k);
		
		prerna.algorithm.impl.LOFReactor alg = new prerna.algorithm.impl.LOFReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
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
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}	
	
	public void runOutlier(String columnName, int numSubsetSize, int numRums, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);

		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.OutlierReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.OutlierReactor.NUM_SAMPLE_SIZE.toUpperCase(), numSubsetSize);
		params.put(prerna.algorithm.impl.OutlierReactor.NUMBER_OF_RUNS.toUpperCase(), numRums);
		
		prerna.algorithm.impl.OutlierReactor alg = new prerna.algorithm.impl.OutlierReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
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
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	public void runSimilarity(String columnName, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);

		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.SimilarityReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		
		prerna.algorithm.impl.SimilarityReactor alg = new prerna.algorithm.impl.SimilarityReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
}
