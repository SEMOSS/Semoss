package prerna.sablecc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.rosuda.JRI.Rengine;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.ds.util.FileIterator.FILE_DATA_TYPE;
import prerna.poi.main.HeadersException;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRJavaReactor extends AbstractJavaReactor {

	public static final String R_CONN = "R_CONN";
	public static final String R_PORT = "R_PORT";
	public static final String R_ENGINE = "R_ENGINE";
	
	public AbstractRJavaReactor() {
		super();
	}
	
	public AbstractRJavaReactor(ITableDataFrame frame) {
		super(frame);
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	/////////////////// Abstract R Methods /////////////////////
	
	/*
	 * These are the methods that we cannot make generic between
	 * the implementations of R
	 */
	
	protected abstract Object startR();
	
	protected abstract Object eval(String script);

	protected abstract void runR(String script);

	protected abstract void runR(String script, boolean outputResult);

	protected abstract void cleanUpR();
	
	protected abstract void endR();
	
	// tinker specific abstract methods
	protected abstract void colorClusters(String clusterName);
	
	protected abstract void key();
	
	protected abstract void synchronizeXY(String rVarName);
	
	// h2 specific abstract methods
	
	protected abstract String[] getColTypes(String frameName, boolean print);

	protected abstract String[] getColNames(String frameName, boolean print);

	protected abstract Object[][] getColumnCount(String frameName, String colName, boolean outputString);

	protected abstract Object[][] getDescriptiveStats(String frameName, String colName, boolean outputString);

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////////// R Methods /////////////////////////
	
	/**
	 * Shift the dataframe into R
	 * @param rVarName
	 */
	public void synchronizeToR(String rVarName) {
		java.lang.System.setSecurityManager(curManager);
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		wd = baseFolder + "/" + randomDir;		
		if(dataframe instanceof TinkerFrame) {
			synchronizeGraphToR(rVarName, wd);
		}
		else if(dataframe instanceof H2Frame) {
			synchronizeGridToR(rVarName);
		}
	}
	
	public void synchronizeFromR()
	{
		if(dataframe instanceof TinkerFrame) {
			String graphName = (String)retrieveVariable("GRAPH_NAME");
			synchronizeGraphFromR(graphName);
		} else if(dataframe instanceof H2Frame) {
			
		}
	}
	
	/**
	 * Install a R package
	 * @param packageName
	 */
	protected void installR(String packageName) {
		startR();
		eval("install.packages('" + packageName + "', repos='http://cran.us.r-project.org');");
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////// H2 R Methods ////////////////////////
	
	/**
	 * Synchronize the grid to R
	 * @param frameName
	 */
	protected void synchronizeGridToR(String rVarName) {
		synchronizeGridToR(rVarName, null);
	}
	
	/**
	 * Synchronize the grid to R
	 * @param frameName
	 * @param cols
	 */
	private void synchronizeGridToR(String frameName, String cols)
	{
		H2Frame gridFrame = (H2Frame)dataframe;
		String tableName = gridFrame.getBuilder().getTableName();
		String url = gridFrame.getBuilder().connectFrame();
		url = url.replace("\\", "/");
		initiateDriver(url, "sa");
		
		String selectors = "*";
		if(cols != null && cols.length() >= 0) {
			selectors = "";
			String [] colSelectors = cols.split(";");
			for(int selectIndex = 0;selectIndex < colSelectors.length;selectIndex++) {
				selectors = selectors + colSelectors[selectIndex];
				if(selectIndex + 1 < colSelectors.length)
					selectors = selectors + ", ";
			}
		}
		
		// make sure R is started
		startR();
		eval(frameName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName + "')));");
		eval("setDT(" + frameName + ")");

		storeVariable("GRID_NAME", frameName);	
		System.out.println("Completed synchronization as " + frameName);
	}
	
	/**
	 * Create a H2Frame from an existing R data table
	 */
	protected void synchronizeGridFromR() {
		String frameName = (String)retrieveVariable("GRID_NAME");
		synchronizeGridFromR(frameName, true);
	}
	
	/**
	 * Synchronize a R data table into a H2Frame
	 * @param frameName
	 * @param overrideExistingTable
	 */
	protected void synchronizeGridFromR(String frameName, boolean overrideExistingTable) {
		// get the necessary information from the r frame
		// to be able to add the data correclty
		
		// get the names and types
		String[] colNames = getColNames(frameName, false);
		// since R has less restrictions than we do regarding header names
		// we will clean the header names to match what the cleaning would be when we load
		// in a file
		// note: the clean routine will only do something if the metadata has changed
		// otherwise, the headers would already be good to go
		List<String> cleanColNames = new Vector<String>();
		HeadersException headerException = HeadersException.getInstance();
		for(int i = 0; i < colNames.length; i++) {
			String cleanHeader = headerException.recursivelyFixHeaders(colNames[i], cleanColNames);
			cleanColNames.add(cleanHeader);
		}
		colNames = cleanColNames.toArray(new String[]{});
		
		String[] colTypes = getColTypes(frameName, false);
		
		// need to create a data type map and a query struct
		QueryStruct qs = new QueryStruct();
		// TODO: REALLY NEED TO CONSOLIDATE THE STRING VS. METADATA TYPE DATAMAPS
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		for(int i = 0; i < colNames.length; i++) {
			dataTypeMapStr.put(colNames[i], colTypes[i]);
			dataTypeMap.put(colNames[i], Utility.convertStringToDataType(colTypes[i]));
			qs.addSelector(colNames[i], null);
		}
		
		/*
		 * logic to determine where we are adding this data...
		 * 1) First, make sure the existing frame is a grid
		 * 		-> If it is not a grid, we already know we need to make a new h2frame
		 * 2) Second, if it is a grid, check the meta data and see if it has changed
		 * 		-> if it has changed, we need to make a new h2frame
		 * 3) Regardless of #2 -> user can decide what they want to create a new frame
		 * 			even if the meta data hasn't changed
		 */
		
		boolean frameIsH2 = false;
		String schemaName = null;
		String tableName = null;
		boolean determineNewFrameNeeded = false;
		
		// if we dont even have a h2frame currently, make a new one
		if( !(dataframe instanceof H2Frame) ) {
			determineNewFrameNeeded = true;
		} else {
			frameIsH2 = true;
			schemaName = ((H2Frame) dataframe).getSchema();
			tableName = ((H2Frame) dataframe).getTableName();
			
			// if we do have an h2frame, look at headers to figure 
			// out if the metadata has changed
			
			String[] currHeaders = dataframe.getColumnHeaders();
			
			if(colNames.length != currHeaders.length) {
				determineNewFrameNeeded = true;
			} else {
				for(String currHeader : currHeaders) {
					if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(colNames, currHeader)) {
						determineNewFrameNeeded = true;
					}
				}
			}
		}
		
		H2Frame frameToUse = null;
		if(!overrideExistingTable || determineNewFrameNeeded) {
			frameToUse = new H2Frame();
			
			// set the correct schema in the new frame
			// drop the existing table
			if(frameIsH2) {
				frameToUse.setUserId(schemaName);
				((H2Frame) dataframe).dropTable();
			}
			
			Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
			frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);
			
			// override frame references & table name reference
			this.put("G", frameToUse);
			dataframe = frameToUse;
			tableName = frameToUse.getTableName();
			
		} else if(overrideExistingTable && frameIsH2){
			frameToUse = ((H2Frame) dataframe);

			// can only enter here we are overriding the existing H2Frame
			// drop any index if altering the existing frame
			Set<String> columnIndices = frameToUse.getColumnsWithIndexes();
			if(columnIndices != null) {
				for(String colName : columnIndices) {
					frameToUse.removeColumnIndex(colName);
				}
			}
			
			// drop all existing data
			frameToUse.deleteAllRows();
		}
		
		// we will make a temp file
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\" + DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		eval("fwrite(" + frameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		FileIterator dataIterator = FileIterator.createInstance(FILE_DATA_TYPE.META_DATA_ENUM, tempFileLocation, ',', qs, dataTypeMap);
		frameToUse.addRowsViaIterator(dataIterator, dataTypeMap);
		dataIterator.deleteFile();
		
		System.out.println("Table Synchronized as " + tableName);
		frameToUse.updateDataId();
	}
	
	protected void initiateDriver(String url, String username) {
		String driver = "org.h2.Driver";
		String jarLocation = "";
		if(retrieveVariable("H2DRIVER_PATH") == null) {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");;
			String jar = "h2-1.4.185.jar"; // TODO: create an enum of available drivers and the necessary jar for each
			jarLocation = workingDir + "/RDFGraphLib/" + jar;
		} else {
			jarLocation = (String)retrieveVariable("H2DRIVER_PATH");
		}
		java.lang.System.out.println("Loading driver.. " + jarLocation);
		String script = "drv <- JDBC('" + driver + "', '" + jarLocation  + "', identifier.quote='`');" 
			+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', '')"; // line of R script that connects to H2Frame
		runR(script);
	}
	
	/**
	 * Synchronize a CSV File into an R Data Table
	 * @param fileName
	 * @param frameName
	 */
	protected void synchronizeCSVToR(String fileName, String frameName) {
		eval(frameName + " <- fread(\"" + fileName + "\")");
		System.out.println("Completed synchronization of CSV " + fileName);
	}
	
	/**
	 * Get the column count of a given column
	 * @param column
	 */
	protected Object[][] getColumnCount(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		return getColumnCount(frameName, colName);
	}
	
	protected Object[][] getColumnCount(String frameName, String colName) {
		return getColumnCount(frameName, colName, true);
	}
	
	/**
	 * Get the column count of a given column
	 * @param column
	 */
	protected Object[][] getDescriptiveStats(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		return getDescriptiveStats(frameName, colName);
	}
	
	protected Object[][] getDescriptiveStats(String frameName, String colName) {
		return getDescriptiveStats(frameName, colName, true);
	}
	
	/**
	 * Drop a column within the table
	 * @param colName
	 */
	protected void dropRColumn(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		dropRColumn(frameName, colName);
	}
	
	protected void dropRColumn(String frameName, String colName) {
		eval(frameName + "[," + colName + ":=NULL]");
		System.out.println("Successfully removed column = " + colName);
	}
	
	/**
	 * Drop rows based on a comparator for a set of values
	 * @param colName
	 * @param comparator
	 * @param values
	 */
	protected void dropRowsWhereColumnContainsValue(String colName, String comparator, Object values) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, values);
	}
	
	/**
	 * Filter out rows based on values in a given column
	 * @param frameName
	 * @param colName
	 * @param comparator
	 * @param values
	 */
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, Object values) {
		// to account for misunderstandings between = and == for normal users
		if(comparator.equals("=")) {
			comparator = "==";
		}
		String frameExpression = frameName + "$" + colName;
		StringBuilder script = new StringBuilder(frameName).append("<-").append(frameName).append("[!(").append(frameExpression);
		if(values instanceof Object[]) {
			Object[] arr = (Object[]) values;
			Object val = arr[0];
			if(val instanceof String) {
				if(val.equals("NULL")) {
					script.append(" | ").append(frameExpression).append(comparator).append(val);
				} else {
					script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val).append("\"");
				}
			} else {
				script.append(comparator).append(val);
			}
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				if(val instanceof String) {
					if(val.equals("NULL")) {
						script.append(" | ").append(frameExpression).append(comparator).append(val);
					} else {
						script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val).append("\"");
					}
				} else {
					script.append(" | ").append(frameExpression).append(comparator).append(val);
				}
			}
		} else if(values instanceof String[]){
			String[] arr = (String[]) values;
			String val = arr[0];
			script.append(comparator).append("\"").append(val).append("\"");
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val).append("\"");
			}
		} else if(values instanceof Double[])  {
			Double[] arr = (Double[]) values;
			Double val = arr[0];
			script.append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof Integer[])  {
			Integer[] arr = (Integer[]) values;
			Integer val = arr[0];
			script.append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof double[])  {
			double[] arr = (double[]) values;
			double val = arr[0];
			script.append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof int[])  {
			int[] arr = (int[]) values;
			int val = arr[0];
			script.append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else {
			if(values instanceof String) {
				if(values.toString().equals("NULL")) {
					script.append(" | ").append(frameExpression).append(comparator).append(values);
				} else {
					script.append(" | ").append(frameExpression).append(comparator).append("\"").append(values).append("\"");
				}
			} else {
				script.append(comparator).append(values);
			}
		}
		script.append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
	}
	
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, int value) {
		// to account for misunderstandings between = and == for normal users
		if(comparator.equals("=")) {
			comparator = "==";
		}
		String frameExpression = frameName + "$" + colName;
		StringBuilder script = new StringBuilder(frameName).append("<-").append(frameName).append("[!(")
				.append(frameExpression).append(comparator).append(value).append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
	}
	
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, double value) {
		// to account for misunderstandings between = and == for normal users
		if(comparator.equals("=")) {
			comparator = "==";
		}
		String frameExpression = frameName + "$" + colName;
		StringBuilder script = new StringBuilder(frameName).append("<-").append(frameName).append("[!(")
				.append(frameExpression).append(comparator).append(value).append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////// Tinker R Methods //////////////////////

	private void synchronizeGraphToR(String graphName, String wd) {
		java.io.File file = new File(wd);
		try {
			// create this directory
			file.mkdir();
			fileName = writeGraph(wd);
			
			LOGGER.info("Trying to start R.. ");
			startR();
			LOGGER.info("Successfully started R");
			
			wd = wd.replace("\\", "/");
			
			// set the working directory
			eval("setwd(\"" + wd + "\")");
			// load the library
			eval("library(\"igraph\");");

			String loadGraphScript = graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			eval(loadGraphScript);
			
			System.out.println("Successfully synchronized, your graph is now available as " + graphName);
			// store the graph name for future use
			storeVariable("GRAPH_NAME", graphName);	
		} catch(Exception ex) {
			ex.printStackTrace();
			System.out.println("ERROR ::: Could not convert TinkerFrame into iGraph.\nPlease make sure iGraph package is installed.");
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Synchronize graph from iGraph
	 * @param graphName
	 */
	private void synchronizeGraphFromR(String graphName) {
		System.out.println("ERROR ::: Have not implemented synchronizeGraphFromR yet...");

		// get the attributes
		// and then synchronize all the different properties
		// vertex_attr_names
//		String names = "";
//		RConnection con = (RConnection)startR();
//
//		// get all the attributes first
//		try {
//			String [] strings = con.eval("vertex_attr_names(" + graphName + ")").asStrings();
//			// the question is do I get everything here and set tinker
//			// or for each get it and so I dont look up tinker many times ?!
//			
//			// now I need to get each of this string and then synchronize
//		} catch (RserveException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (REXPMismatchException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	/**
	 * Remove all nodes of a specific type and with a specific value
	 * @param type
	 * @param data
	 */
	protected void removeNode(String type, String data) {
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
	
	/**
	 * Delete nodes from R iGraph
	 * @param type
	 * @param nodeList
	 */
	protected void removeNodeFromR(String type, List<Object> nodeList) {
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		if(graphName == null) {
			// we will not have a graph name if the graph has not been synchronized to R
			return;
		}
		for(int nodeIndex = 0;nodeIndex < nodeList.size();nodeIndex++) {
			String name = type + ":" + nodeList.get(nodeIndex);
			try{
				java.lang.System.out.println("Deleting node = " + name);
				// eval is abstract and is determined by the specific R implementation
				eval(graphName + " <- delete_vertices(" + graphName + ", V(" + graphName + ")[vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\") == \"" + name + "\"])");				
			} catch(Exception ex) {
				java.lang.System.out.println("ERROR ::: Could not delete node = " + name);
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Perform clusters routine on iGraph
	 */
	protected void clusterInfo() {
		String clusters = "clusters";
		clusterInfo(clusters);
	}
	
	/**
	 * Perform clusters routine on iGraph
	 */
	protected void clusterInfo(String clusterRoutine) {
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		if(graphName == null) {
			System.out.println("ERROR ::: No graph has been synchronized to R");
			return;
		}
		startR();
		try {
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			eval("clus <- " + clusterRoutine + "(" + graphName +")");
			System.out.println("\n No. Of Components :");
			runR("clus$no");
			System.out.println("\n Component Sizes :");
			runR("clus$csize");
			colorClusters();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Perform cluster_walktrap routine on iGraph
	 */
	protected void walkInfo() {
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		
		Rengine retEngine = (Rengine)startR();
		String clusters = "Component Information  \n";
		try {
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			retEngine.eval("clus <- cluster_walktrap(" + graphName +", membership=TRUE)");
			clusters = clusters + "Completed Walktrap";
			System.out.println(clusters);
			colorClusters();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Color tinker nodes based on iGrpah values
	 */
	protected void colorClusters() {
		String clusterName = (String)retrieveVariable("CLUSTER_NAME");
		colorClusters(clusterName);
	}
	
	/**
	 * Serialize the TinkerGraph in GraphML format
	 * @param directory
	 * @return
	 */
	public String writeGraph(String directory) {
		String absoluteFileName = null;
		if(dataframe instanceof TinkerFrame) {
	    	final Graph graph = ((TinkerFrame)dataframe).g;
	    	absoluteFileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
	    	String fileName = directory + "/" + absoluteFileName; 
	    	OutputStream os = null;
	    	try {
	    		os = new FileOutputStream(fileName);
	    	    graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
	    	} catch(Exception ex) {
	    		ex.printStackTrace();
	    	} finally {
	    		try {
	    			if(os != null) {
	    				os.close();
	    			}
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	}
		}
		return absoluteFileName;
	}
	
	/**
	 * Run a layout in iGraph and store back into tinker objects
	 * Possible values: 
	 * 		Fruchterman - layout_with_fr
	 * 		KK - layout_with_kk
	 * 		sugiyama - layout_with_sugiyama
	 * 		layout_as_tree
	 * 		layout_as_star
	 * 		layout.auto
	 * 		http://igraph.org/r/doc/layout_with_fr.html
	 * @param layout
	 */
	public void doLayout(String layout)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		// the color is saved as color
		try {
			eval("xy_layout <- " + layout + "(" + graphName +")");
			synchronizeXY("xy_layout");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
