package prerna.sablecc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

import cern.colt.Arrays;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.ICache;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
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
	public static final String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";
	
	private static long counter = 0;
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

	protected abstract String getWd();
	
	protected abstract void cleanUpR();
	
	protected abstract void endR();
	
	// graph specific abstract methods
	protected abstract void colorClusters(String clusterName);
	
	protected abstract void key();
	
	protected abstract void synchronizeXY(String rVarName);
	
	// table specific abstract methods
	
	protected abstract int getNumRows(String frameName);
	
	protected abstract String getColType(String frameName, String colName, boolean print);
	
	protected abstract String[] getColTypes(String frameName, boolean print);

	protected abstract String[] getColNames(String frameName, boolean print);

	protected abstract Object[][] getColumnCount(String frameName, String colName, boolean outputString);

	protected abstract Object[][] getDescriptiveStats(String frameName, String colName, boolean outputString);

	protected abstract Object[][] getHistogram(String frameName, String column, int numBreaks, boolean print);
	
	protected abstract void performSplitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace);
	
	protected abstract void performJoinColumns(String frameName, String newColumnName,  String separator, String cols);
	
	protected abstract void unpivot(String frameName, String cols, boolean replace);

	protected abstract void pivot(String frameName, boolean replace, String columnToPivot, String cols);
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////////// R Methods /////////////////////////
	
	/**
	 * Shift the dataframe into R with a default name
	 */
	public void synchronizeToR() {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame) {
			synchronizeGraphToR();
		}
		else if(dataframe instanceof H2Frame) {
			synchronizeGridToR();
		}
	}
	
	private static String getDefaultName() {
		//TODO: need to check variable names
		//make sure default name won't override
		return "df_"+counter++;
	}
	
	/**
	 * Shift the dataframe into R
	 * @param rVarName
	 */
	public void synchronizeToR(String rVarName) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame) {
			synchronizeGraphToR(rVarName);
		}
		else if(dataframe instanceof H2Frame) {
			synchronizeGridToR(rVarName);
		}
	}
	
	public void synchronizeFromR() {
		if(dataframe instanceof TinkerFrame) {
			String graphName = (String)retrieveVariable("GRAPH_NAME");
			synchronizeGraphFromR(graphName);
		} else if(dataframe instanceof H2Frame) {
			String frameName = (String)retrieveVariable("GRID_NAME");
			synchronizeGridFromR(frameName, true);
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
	
	protected void synchronizeGridToR() {
		String defaultName = getDefaultName();
		synchronizeGridToR(defaultName);
	}
	
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
		
		// note : do not use * since R will not preserve the column order
		StringBuilder selectors = new StringBuilder();
		String [] colSelectors = null;
		if(cols == null || cols.length() == 0) {
			colSelectors = gridFrame.getColumnHeaders();
		} else {
			colSelectors = cols.split(";");
		}
		
		for(int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
			selectors.append(colSelectors[selectIndex]);
			if(selectIndex + 1 < colSelectors.length) {
				selectors.append(", ");
			}
		}
		
		// make sure R is started
		startR();
		eval(frameName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName + "')));");
		eval("setDT(" + frameName + ")");
		// modify the headers to be what they used to be because the query return everything in 
		// all upper case which may not be accurate
		String[] currHeaders = getColNames(frameName, false);
		renameColumn(frameName, currHeaders, colSelectors, false);
		storeVariable("GRID_NAME", frameName);	
		System.out.println("Completed synchronization as " + frameName);
	}
	
	/**
	 * Synchronize current H2Frame into a R Data Table Frame
	 * @param rVarName
	 */
	protected void synchronizeGridToRDataTable(String rVarName) {
		// if there is a current r serve session
		// use that for the frame so we have all the other variables
		RDataTable table = null;
		if(retrieveVariable(R_CONN) != null && retrieveVariable(R_PORT) != null) {
			table = new RDataTable(rVarName, (RConnection) retrieveVariable(R_CONN), (String) retrieveVariable(R_PORT));
		} else {
			// if we dont have a current r session
			// but when we create the table it makes one
			// store those variables so we end up using that
			table = new RDataTable(rVarName);
			if(table.getConnection() != null && table.getPort() != null) {
				storeVariable(R_CONN, table.getConnection());
				storeVariable(R_PORT, table.getPort());
			}
		}
		
		// should pass the user id down to the frame
		// important when we merge back the frame
		table.setUserId(dataframe.getUserId());
		
		H2Frame gridFrame = (H2Frame)dataframe;
		String tableName = gridFrame.getBuilder().getTableName();
		String url = gridFrame.getBuilder().connectFrame();
		url = url.replace("\\", "/");
		initiateDriver(url, "sa");

		
		// need to create a new data table
		// should properly merge the meta data
		Map<String, Set<String>> edgeHash = gridFrame.getEdgeHash();
		Map<String, String> dataTypeMap = new HashMap<String, String>();
		for(String colName : edgeHash.keySet()) {
			if(!dataTypeMap.containsKey(colName)) {
				dataTypeMap.put(colName, Utility.convertDataTypeToString(gridFrame.getDataType(colName)) );
			}
			
			Set<String> otherCols = edgeHash.get(colName);
			for(String otherCol : otherCols) {
				if(!dataTypeMap.containsKey(otherCol)) {
					dataTypeMap.put(otherCol, Utility.convertDataTypeToString(gridFrame.getDataType(otherCol)) );
				}
			}
		}
		
		table.mergeEdgeHash(edgeHash, dataTypeMap);

		StringBuilder selectors = new StringBuilder();
		String [] colSelectors = gridFrame.getColumnHeaders();
		for(int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
			selectors.append(colSelectors[selectIndex]);
			if(selectIndex + 1 < colSelectors.length) {
				selectors.append(", ");
			}
		}
		
		eval(rVarName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName + "')));");
		eval("setDT(" + rVarName + ")");
		
		// modify the headers to be what they used to be because the * will return everything in caps
		String[] currHeaders = getColNames(rVarName, false);
		renameColumn(rVarName, currHeaders, colSelectors, false);
		storeVariable("GRID_NAME", rVarName);
		System.out.println("Completed synchronization as " + rVarName);

		this.dataframe = table;
		this.frameChanged = true;
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
		// to be able to add the data correctly
		
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
			} else {
				// this is set when we set the original dataframe
				// within the reactor
				frameToUse.setUserId(this.userId);
			}
			
			Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
			frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);
			
			// override frame references & table name reference
			this.put("G", frameToUse);
			this.dataframe = frameToUse;
			this.frameChanged = true;
			tableName = frameToUse.getTableName();
			
		} else if(overrideExistingTable && frameIsH2){
			frameToUse = ((H2Frame) dataframe);

			// can only enter here when we are overriding the existing H2Frame
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
		
		// keep track of in-mem vs on-disk frames
		int limitSizeInt = 10_000 / colNames.length;
		String limitSize = (String) DIHelper.getInstance().getProperty(Constants.H2_IN_MEM_SIZE);
		if(limitSize != null) {
			limitSizeInt = Integer.parseInt( limitSize.trim() );
		}
		if(dataIterator.numberRowsOverLimit(limitSizeInt) ) {
			frameToUse.convertToOnDiskFrame(null);
		}
		
		// now that we know if we are adding to disk vs mem
		// iterate through and add all the data
		frameToUse.addRowsViaIterator(dataIterator, dataTypeMap);
		dataIterator.deleteFile();
		
		System.out.println("Table Synchronized as " + tableName);
		frameToUse.updateDataId();
	}
	
	/**
	 * Synchronize a R data table into a H2Frame
	 * @param frameName
	 * @param overrideExistingTable
	 */
	protected void synchronizeGridFromRDataTable(String frameName) {
		synchronizeGridFromR(frameName, true);
//		RDataTable rFrame = (RDataTable) dataframe;
//		Map<String, Set<String>> edgeHash = rFrame.getEdgeHash();
//		
//		// need to create a data type map and a query struct
//		QueryStruct qs = new QueryStruct();
//		// TODO: REALLY NEED TO CONSOLIDATE THE STRING VS. METADATA TYPE DATAMAPS
//		Map<String, Set<String>> cleanEdgeHash = new HashMap<String, Set<String>>();
//		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new HashMap<String, IMetaData.DATA_TYPES>();
//		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
//		for(String colName : edgeHash.keySet()) {
//			String cleanColName = colName.toUpperCase();
//			if(!dataTypeMap.containsKey(cleanColName)) {
//				DATA_TYPES type = rFrame.getDataType(colName);
//				dataTypeMap.put(cleanColName, type);
//				dataTypeMapStr.put(cleanColName, Utility.convertDataTypeToString(type) );
//				qs.addSelector(cleanColName, null);
//			}
//			
//			Set<String> cleanOtherCols = new HashSet<String>();
//			Set<String> otherCols = edgeHash.get(colName);
//			for(String otherCol : otherCols) {
//				String cleanOtherCol = otherCol.toUpperCase();
//				if(!dataTypeMap.containsKey(cleanOtherCol)) {
//					DATA_TYPES type = rFrame.getDataType(colName);
//					dataTypeMap.put(cleanOtherCol, type );
//					dataTypeMapStr.put(cleanOtherCol, Utility.convertDataTypeToString(type) );
//					qs.addSelector(cleanOtherCol, null);
//				}
//				cleanOtherCols.add(cleanOtherCol);
//			}
//			
//			cleanEdgeHash.put(cleanColName, cleanOtherCols);
//		}
//		
//		// we will make a temp file
//		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\" + DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
//		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
//		tempFileLocation = tempFileLocation.replace("\\", "/");
//		eval("fwrite(" + frameName + ", file='" + tempFileLocation + "')");
//		
//		H2Frame table = new H2Frame();
//		table.mergeEdgeHash(cleanEdgeHash, dataTypeMapStr);
//
//		// iterate through file and insert values
//		FileIterator dataIterator = FileIterator.createInstance(FILE_DATA_TYPE.META_DATA_ENUM, tempFileLocation, ',', qs, dataTypeMap);
//		table.addRowsViaIterator(dataIterator, dataTypeMap);
//		dataIterator.deleteFile();
//		
//		System.out.println("Table Synchronized as " + table.getTableName());
//		
//		this.dataframe = table;
//		this.frameChanged = true;
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
		LOGGER.info("Loading driver.. " + jarLocation);
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
	
	protected String getColType(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		return getColType(frameName, colName, true);
	}
	
	protected String getColType(String frameName, String colName) {
		return getColType(frameName, colName, true);
	}
	
	protected String[] getColTypes(String frameName) {
		return getColTypes(frameName, true);
	}
	
	protected String[] getColNames(String frameName) {
		return getColNames(frameName, true);
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
	
	public void getHistogram(String frameName, String column) {
		getHistogram(frameName, column, true);
	}
	
	public void getHistogram(String frameName, String column, boolean print) {
		getHistogram(frameName, column, 0, print);
	}
	
	/**
	 * Add an empty column to later insert new values
	 * @param newColName
	 */
	protected void addEmptyColumn(String newColName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		addEmptyColumn(frameName, newColName);
	}
	
	protected void addEmptyColumn(String frameName, String newColName) {
		String script = frameName + "$" + newColName + " <- \"\" ";
		eval(script);
		System.out.println("Successfully added column = " + newColName);
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	/**
	 * Add an empty column to later insert new values
	 * @param newColName
	 */
	protected void changeColumnType(String colName, String newType) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		changeColumnType(frameName, colName, newType);
	}
	
	protected void changeColumnType(String frameName, String colName, String newType) {
		changeColumnType(frameName, colName, newType, "%Y/%m/%d");
	}
	
	protected void changeColumnType(String frameName, String colName, String newType, String dateFormat) {
		String script = null;
		if(newType.equalsIgnoreCase("string")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.character(" + colName +")]";
			eval(script);
		} else if(newType.equalsIgnoreCase("factor")) { 
			script = frameName + " <- " + frameName + "[, " + colName + " := as.factor(" + colName +")]";
			eval(script);
		} else if(newType.equalsIgnoreCase("number")) {
			script =  frameName + " <- " + frameName + "[, " + colName + " := as.numeric(" + colName +")]";
			eval(script);
		} else if(newType.equalsIgnoreCase("date")) {
			// we have a different script to run if it is a str to date conversion
			// or a date to new date format conversion
			String type = getColType(frameName, colName, false);
			String tempTable = Utility.getRandomString(6);
			if(type.equalsIgnoreCase("date")) {
				String formatString = ", format = '" + dateFormat + "'";
				script = tempTable + " <- format(" + frameName + "$" + colName + formatString + ")";
				eval(script);
				script = frameName + "$" + colName + " <- " + "as.Date(" + tempTable + formatString + ")";
				eval(script);
			} else {
				script =  tempTable + " <- as.Date(" + frameName + "$" + colName + ", format='" + dateFormat + "')";
				eval(script);
				script = frameName + "$" + colName + " <- " + tempTable;
				eval(script);
			}
			// perform variable cleanup
			eval("rm(" + tempTable + ");");
			eval("gc();");		
		}
		System.out.println("Successfully changed data type for column = " + colName);
		if(checkRTableModified(frameName)) {
			// TODO: should be able to change the data type dynamically!!!
			// TODO: come back and fix this
			recreateMetadata(frameName);
		}
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
		if(checkRTableModified(frameName)) {
			// this R method will do the same evaluation
			// but it will also drop it from the metadata
			this.dataframe.removeColumn(colName);
		} else {
			eval(frameName + "[," + colName + ":=NULL]");
		}
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
		if(comparator.trim().equals("=")) {
			comparator = " == ";
		}
		String frameExpression = frameName + "$" + colName;
		// determine the correct comparison to drop values from the frame
		// .... this is a bunch of casting...
		// also note that the string NULL is special to remove values that are undefined within the frame
		StringBuilder script = new StringBuilder(frameName).append(" <- ").append(frameName).append("[!( ");
		String dataType = getColType(frameName, colName, false);
		
		// accommodate for factors cause they are annoying
		if(dataType.equals("factor")) {
			changeColumnType(frameName, colName, "STRING");
			dataType = "character";
		}
		
		if(values instanceof Object[]) {
			Object[] arr = (Object[]) values;
			Object val = arr[0];
			if(dataType.equalsIgnoreCase("character")) {
				if(val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
					script.append("is.na(").append(frameExpression).append(") ");
				} else {
					script.append(frameExpression).append(comparator).append("\"").append(val).append("\"");
				}
			} else {
				script.append(comparator).append(val);
			}
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				if(dataType.equalsIgnoreCase("character")) {
					if(val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
						script.append(" | is.na(").append(frameExpression).append(") ");
					} else {
						script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val).append("\"");
					}
				} else {
					script.append(" | ").append(frameExpression).append(comparator).append(val);
				}
			}
		} else if(values instanceof Double[])  {
			Double[] arr = (Double[]) values;
			Double val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof Integer[])  {
			Integer[] arr = (Integer[]) values;
			Integer val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof double[])  {
			double[] arr = (double[]) values;
			double val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if(values instanceof int[])  {
			int[] arr = (int[]) values;
			int val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for(int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else {
			if(dataType.equalsIgnoreCase("character")) {
				if(values.toString().equalsIgnoreCase("NULL") || values.toString().equalsIgnoreCase("NA")) {
					script.append("is.na(").append(frameExpression).append(") ");
				} else {
					script.append(frameExpression).append(comparator).append("\"").append(values).append("\"");
				}
			} else {
				script.append(frameExpression).append(comparator).append(values);
			}
		}
		script.append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
		checkRTableModified(frameName);
	}
	
	protected void dropRowsWhereColumnContainsValue(String colName, String comparator, int value) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, value);
	}
	
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, int value) {
		// to account for misunderstandings between = and == for normal users
		if(comparator.trim().equals("=")) {
			comparator = " == ";
		}
		String frameExpression = frameName + "$" + colName;
		StringBuilder script = new StringBuilder(frameName).append("<-").append(frameName).append("[!(")
				.append(frameExpression).append(comparator).append(value).append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
		checkRTableModified(frameName);
	}
	
	protected void dropRowsWhereColumnContainsValue(String colName, String comparator, double value) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, value);
	}
	
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, double value) {
		// to account for misunderstandings between = and == for normal users
		if(comparator.trim().equals("=")) {
			comparator = " == ";
		}
		String frameExpression = frameName + "$" + colName;
		StringBuilder script = new StringBuilder(frameName).append("<-").append(frameName).append("[!(")
				.append(frameExpression).append(comparator).append(value).append("),]");
		eval(script.toString());
		System.out.println("Script ran = " + script.toString() + "\nSuccessfully removed rows");
		checkRTableModified(frameName);
	}
	
	/**
	 * Create a new column by counting the presence of a string within another column
	 * @param newColName
	 * @param countColName
	 * @param strToCount
	 */
	protected void insertStrCountColumn(String newColName, String countColName, String strToCount) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		insertStrCountColumn(frameName, newColName, countColName, strToCount);
	}
	
	protected void insertStrCountColumn(String newColName, String countColName, int valToCount) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		insertStrCountColumn(frameName, newColName, countColName, valToCount + "");
	}
	
	protected void insertStrCountColumn(String newColName, String countColName, double valToCount) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		insertStrCountColumn(frameName, newColName, countColName, valToCount + "");
	}
	
	protected void insertStrCountColumn(String frameName, String newColName, String countColName, int valToCount) {
		insertStrCountColumn(frameName, newColName, countColName, valToCount + "");
	}
	
	protected void insertStrCountColumn(String frameName, String newColName, String countColName, double valToCount) {
		insertStrCountColumn(frameName, newColName, countColName, valToCount + "");
	}
	
	protected void insertStrCountColumn(String frameName, String newColName, String countColName, Object strToCount) {
		// dt$new <- str_count(dt$oldCol, "strToFind");
		String script = frameName + "$" + newColName + " <- str_count(" + frameName + "$" + countColName + ", \"" + strToCount + "\")";
		eval(script);
		System.out.println("Added new column = " + newColName);
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	/**
	 * Turn a string to lower case
	 * @param colName
	 */
	protected void toLowerCase(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		toLowerCase(frameName, colName);
	}
	
	protected void toLowerCase(String frameName, String colName) {
		String script = frameName + "$" + colName + " <- tolower(" + frameName + "$" + colName + ")";
		eval(script);
		checkRTableModified(frameName);
	}
	
	/**
	 * Turn a string to lower case
	 * @param colName
	 */
	protected void toUpperCase(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		toUpperCase(frameName, colName);
	}
	
	protected void toUpperCase(String frameName, String colName) {
		String script = frameName + "$" + colName + " <- toupper(" + frameName + "$" + colName + ")";
		eval(script);
		checkRTableModified(frameName);
	}
	
	/**
	 * Turn a string to lower case
	 * @param colName
	 */
	protected void trim(String colName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		trim(frameName, colName);
	}
	
	protected void trim(String frameName, String colName) {
		String script = frameName + "$" + colName + " <- str_trim(" + frameName + "$" + colName + ")";
		eval(script);
		checkRTableModified(frameName);
	}

	/**
	 * Replace a column value with a new value
	 * @param columnName
	 * @param curValue
	 * @param newValue
	 */
	protected void replaceColumnValue(String columnName, String curValue, String newValue) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		replaceColumnValue(frameName, columnName, curValue, newValue);
	}
	
	protected void replaceColumnValue(String frameName, String columnName, String curValue, String newValue) {
		// replace the column value for a particular column
		// dt[PY == "hello", PY := "D"] replaces a column conditionally based on the value
		// need to get the type of this
		try {
			String condition = " ,";
			String dataType = getColType(columnName);
			String quote = "";
			if(dataType.contains("character")) {
				quote = "\"";
			} else if(dataType.equals("factor")) {
				changeColumnType(frameName, columnName, "STRING");
				quote = "\"";
			}
			if(curValue.equalsIgnoreCase("null") || curValue.equalsIgnoreCase("NA")) {
				condition = "is.na(" + columnName + ") , ";
			} else {
				condition = columnName + " == " + quote + curValue + quote + ", ";
			}
			String script = frameName + "[" + condition + columnName + " := " + quote + newValue + quote + "]";
			eval(script);
			System.out.println("Done replacing value = \"" + curValue + "\" with new value = \"" + newValue + "\"");
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkRTableModified(frameName);
	}
	
	/**
	 * 
	 * @param frameName
	 * @param columnName
	 * @param curValue
	 * @param newValue
	 */
	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue, String conditionalColName, String comparator, Object conditionalColValue) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName, comparator, conditionalColValue);
	}
	
	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue, String conditionalColName, String comparator, double conditionalColValue) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColValue, conditionalColName, comparator, conditionalColValue + "");
	}
	
	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue, String conditionalColName, String comparator, int conditionalColValue) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColValue, conditionalColName, comparator, conditionalColValue + "");
	}
	
	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName, Object updateColValue, String conditionalColName, String comparator, double conditionalColValue) {
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName, comparator, conditionalColValue + "");
	}
	
	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName, Object updateColValue, String conditionalColName, String comparator, int conditionalColValue) {
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName, comparator, conditionalColValue + "");
	}
	
	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName, Object updateColValue, String conditionalColName, String comparator, Object conditionalColValue) {
		// update values based on other columns
		// dt$updateColName[dt$conditionalColName == "conditionalColValue] <- updateColValue
		// need to get the types of this
		try {
			if(comparator.trim().equals("=")) {
				comparator = "==";
			}
			comparator = " " + comparator + " ";
			
			String updateDataType = getColType(updateColName);
			String updateQuote = "";
			if(updateDataType.contains("character")) {
				updateQuote = "\"";
			}
			
			String conditionColDataType = getColType(conditionalColName);
			String conditionColQuote = "";
			if(conditionColDataType.contains("character")) {
				conditionColQuote = "\"";
			}
			
			String script = frameName + "$" + updateColName + "[" + frameName + "$" + conditionalColName + comparator + 
					conditionColQuote + conditionalColValue +  conditionColQuote + "] <- " +  updateQuote + updateColValue + updateQuote;
			eval(script);
			System.out.println("Done updating column " + updateColName + " where " + conditionalColName + comparator + conditionalColValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkRTableModified(frameName);
	}
	
	/**
	 * Regex replace a column value with a new value
	 * @param columnName
	 * @param curValue
	 * @param newValue
	 */
	protected void regexReplaceColumnValue(String columnName, String regex, String newValue) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		regexReplaceColumnValue(frameName, columnName, regex, newValue);
	}
	
	protected void regexReplaceColumnValue(String frameName, String columnName, String regex, String newValue) {
		// replace the column value for a particular column
		// dt$Title = gsub("regex", "newValue", dt$Title)
		// need to get the type of this
		try {
			String colScript = frameName + "$" + columnName;
			String script = colScript + " = ";
			String dataType = getColType(columnName);
			String quote = "";
			if(dataType.contains("character")) {
				quote = "\"";
			}
			script += "gsub(" + quote + regex + quote + "," + quote + newValue + quote + ", " + colScript + ")";
			eval(script);
			System.out.println("Done replacing value with regex = \"" + regex + "\" with new value = \"" + newValue + "\"");
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkRTableModified(frameName);
	}
	
	
	protected void splitColumn(String columnName, String separator) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		splitColumn(frameName, columnName, separator, false, true);
	}
	
	protected void splitColumn(String frameName, String columnName, String separator) {
		splitColumn(frameName, columnName, separator, false, true);
	}
	
	protected void splitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace) {
		performSplitColumn(frameName, columnName, separator, false, true);
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	protected void joinColumns(String newColumnName, String separator, String cols) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		joinColumns(frameName, newColumnName, separator, cols);
	}
	
	protected void joinColumns(String frameName, String newColumnName, String separator, String cols) {
		performJoinColumns(frameName, newColumnName, separator, cols);
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	protected void transpose() {
		String frameName = (String)retrieveVariable("GRID_NAME");
		transpose(frameName);
	}
	
	protected void transpose(String frameName) {
		String script = frameName + " <- " + frameName + "[, data.table(t(.SD), keep.rownames=TRUE)]";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully transposed data table into existing frame");
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	protected void transpose(String frameName, String transposeFrameName) {
		String script = transposeFrameName + " <- " + frameName + "[, data.table(t(.SD), keep.rownames=TRUE)]";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully transposed data table into new frame " + transposeFrameName);
		if(checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	protected void unpivot() {
		String frameName = (String)retrieveVariable("GRID_NAME");
		unpivot(frameName, null, true);
	}
	
	protected void pivot(String columnToPivot, String cols) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		pivot(frameName, true, columnToPivot, cols);
	}
	
	protected void recreateMetadata(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColNames(frameName, false);
		String[] colTypes = getColTypes(frameName, false);
		
		// create the data type map and the edge hash
		Map<String, String> dataTypeMap = new Hashtable<String, String>();
		for(int i = 0; i < colNames.length; i++) {
			dataTypeMap.put(colNames[i], Utility.getCleanDataType(colTypes[i]));
		}
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);

		// create a new table using the correct variables
		// to get into this point
		// we know there is an existing r table
		// so if there is no r_conn + port variables
		// it must be JRI
		RDataTable newTable = null;
		if(retrieveVariable(R_CONN) != null && retrieveVariable(R_PORT) != null) {
			newTable = new RDataTable(frameName, (RConnection) retrieveVariable(R_CONN), (String) retrieveVariable(R_PORT));
		} else {
			newTable = new RDataTable(frameName);
		}
		newTable.mergeEdgeHash(edgeHash, dataTypeMap);
		
		int currDataId = this.dataframe.getDataId();
		for(int i = 0; i <= currDataId; i++) {
			newTable.updateDataId();
		}
		
		this.dataframe = newTable;
		this.frameChanged = true;
	}

	protected void renameColumn(String curColName, String newColName) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		renameColumn(frameName, curColName, newColName);
	}
	
	protected void renameColumn(String frameName, String curColName, String newColName) {
		String script = "names(" + frameName + ")[names(" + frameName + ") == \"" + curColName + "\"] = \"" + newColName + "\"";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully modified name = " + curColName + " to now be " + newColName);
		if(checkRTableModified(frameName)) {
			this.dataframe.modifyColumnName(curColName, newColName);
		}
	}
	
	protected void renameColumn(String[] oldNames, String[] newColNames) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		renameColumn(frameName, oldNames, newColNames);
	}
	
	protected void renameColumn(String frameName, String[] oldNames, String[] newColNames) {
		renameColumn(frameName, oldNames, newColNames, true);
	}
	
	protected void renameColumn(String frameName, String[] oldNames, String[] newNames, boolean print) {
		int size = oldNames.length;
		if(size != newNames.length) {
			throw new IllegalArgumentException("Names arrays do not match in length");
		}
		StringBuilder oldC = new StringBuilder("c(");
		int i = 0;
		oldC.append("'").append(oldNames[i]).append("'");
		i++;
		for(; i < size; i++) {
			oldC.append(", '").append(oldNames[i]).append("'");
		}
		oldC.append(")");
		
		StringBuilder newC = new StringBuilder("c(");
		i = 0;
		newC.append("'").append(newNames[i]).append("'");
		i++;
		for(; i < size; i++) {
			newC.append(", '").append(newNames[i]).append("'");
		}
		newC.append(")");
		
		String script = "setnames(" + frameName + ", old = " + oldC + ", new = " + newC + ")";
		eval(script);

		if(print) {
			System.out.println("Running script : " + script);
			System.out.println("Successfully modified old names = " + Arrays.toString(oldNames) + " to new names " + Arrays.toString(newNames));
		}
		if(checkRTableModified(frameName)) {
			for(i = 0; i < size; i++) {
				this.dataframe.modifyColumnName(oldNames[i], newNames[i]);
			}
		}
	}
	
	/**
	 * Modify the specific cell value in the data frame
	 * @param colName
	 * @param rowNum
	 * @param newVal
	 */
	protected void modifyCellValues(String colName, int rowNum, Object newVal) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}
	
	protected void modifyCellValues(String colName, int rowNum, int newVal) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}
	
	protected void modifyCellValues(String colName, int rowNum, double newVal) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}
	
	protected void modifyCellValues(String frameName, String colName, int rowNum, Object newVal) {
		String type = getColType(frameName, colName, false);
		if(type.contains("character")) {
			newVal = "\"" + newVal + "\"";
		}
		String script = frameName + "[" + rowNum + "]$" + colName + " <- " + newVal;
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}
	
	protected void modifyCellValues(String frameName, String colName, int rowNum, int newVal) {
		String type = getColType(frameName, colName, false);
		String value = newVal + "";
		if(type.contains("character")) {
			value = "\"" + newVal + "\"";
		}
		String script = frameName + "[" + rowNum + "]$" + colName + " <- " + value;
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}
	
	protected void modifyCellValues(String frameName, String colName, int rowNum, double newVal) {
		String type = getColType(frameName, colName, false);
		String value = newVal + "";
		if(type.contains("character")) {
			value = "\"" + newVal + "\"";
		}
		String script = frameName + "[" + rowNum + "]$" + colName + " <- " + value;
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}

	/**
	 * If we order the data, we need to maintain that structure within the entire grid
	 * If we are to actually be able to replace values based on index
	 * @param colName
	 * @param orderDirection
	 */
	protected void sortData(String colName, String orderDirection) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		sortData(frameName, colName, orderDirection);
	}
	
	protected void sortData(String frameName, String colName, String orderDirection) {
		String script = null;
		if(orderDirection == null || orderDirection.equalsIgnoreCase("desc")) {
			script = frameName + " <- " + frameName + "[order(rank(" + colName + "))]";
		} else if(orderDirection.equalsIgnoreCase("asc")) {
			script = frameName + " <- " + frameName + "[order(-rank(" + colName + "))]";
		}
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}
	
	/**
	 * Insert data at a given index into the frame
	 * @param index
	 * @param values
	 */
	protected void insertDataAtIndex(int index, Object[] values) {
		String frameName = (String)retrieveVariable("GRID_NAME");
		insertDataAtIndex(frameName, index, values);
	}

	protected void insertDataAtIndex(String frameName, int index, Object[] values) {
		// we create a string with the correct types of the values array
		// and then we use that to do the rbindlist
		// if we use a conventional vector with c
		// it will require all the same types
		
		String[] names = getColNames(frameName, false);
		String[] types = getColTypes(frameName, false);
		
		String listName = Utility.getRandomString(6);
		StringBuilder listScript = new StringBuilder(listName).append(" <- list(");
		for(int i = 0; i < values.length; i++) {
			if(i > 0) {
				listScript.append(", ");
			}
			listScript.append(names[i]).append("=");
			if(types[i].equalsIgnoreCase("character")) {
				listScript.append("\"").append(values[i]).append("\"");
			} else {
				listScript.append(values[i]);
			}
		}
		listScript.append(")");
		eval(listScript.toString());

		String script = null;
		int totalRows = getNumRows(frameName);
		if(index == 1) {
			script = frameName + " <- rbindlist(list( " + listName + ", " + frameName + " ))";
		} else if(index == (totalRows + 1) ) {
			script = frameName + " <- rbindlist(list( " + frameName + ", " + listName + " ))";
		} else {
			// ugh... somewhere in the middle
			script = frameName + " <- rbindlist(list(" + frameName + "[1:" + (index-1) + ",] , " + listName + " , " 
					+  frameName + "[" + index + ":" + totalRows + ",] ))";
		}
		eval(script);
		System.out.println("Running script :\n" + listScript + "\n" + script);

		checkRTableModified(frameName);
	}
	
	protected boolean checkRTableModified(String frameName) {
		if(this.dataframe instanceof RDataTable) {
			String tableVarName =  ( (RDataTable) this.dataframe).getTableVarName();
			if(frameName.equals(tableVarName)) {
				this.dataframe.updateDataId();
				return true;
			}
		}
		return false;
	}
	
	protected Map<String, Object> getBarChartInfo(String label, String value, Object[][] dataValues) {
		// create the weird object the FE needs to paint a bar chart
		Map<String, Object>[] keyMap = new Hashtable[2];
		Map<String, Object> labelMap = new Hashtable<String, Object>();
		labelMap.put("vizType", "label");
		labelMap.put("type", "STRING");
		labelMap.put("uri", label);
		labelMap.put("varKey", label);
		labelMap.put("operation", new Hashtable());
		keyMap[0] = labelMap;
		Map<String, Object> frequencyMap = new Hashtable<String, Object>();
		frequencyMap.put("vizType", "value");
		frequencyMap.put("type", "NUMBER");
		frequencyMap.put("uri", value);
		frequencyMap.put("varKey", value);
		frequencyMap.put("operation", new Hashtable());
		keyMap[1] = frequencyMap;
		
		Map<String, Object> retObj = new Hashtable<String, Object>();
		retObj.put("dataTableKeys", keyMap);
		retObj.put("dataTableValues", dataValues);
		
		return retObj;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////// Tinker R Methods //////////////////////

	protected void synchronizeGraphToR() {
		String defaultName = getDefaultName();
		synchronizeGraphToR(defaultName);
	}
	
	protected void synchronizeGraphToR(String rVarName) {
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		wd = baseFolder + "/" + randomDir;
		synchronizeGraphToR(rVarName, wd);
	}
	
	private void synchronizeGraphToR(String graphName, String wd) {
		java.io.File file = new File(wd);
		String curWd = null;
		try {
			LOGGER.info("Trying to start R.. ");	
			startR();
			LOGGER.info("Successfully started R");
			
			// get the current directory
			// we need to switch out of this to write the graph file
			// but want to go back to this original one
			curWd = getWd();

			// create this directory
			file.mkdir();
			fileName = writeGraph(wd);
			
			wd = wd.replace("\\", "/");
			
			// set the working directory
			eval("setwd(\"" + wd + "\")");
			// load the library
			Object ret = eval("library(\"igraph\");");
			if(ret == null) {
				ICache.deleteFolder(wd);
				throw new ClassNotFoundException("Package igraph could not be found!");
			}
			String loadGraphScript = graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			eval(loadGraphScript);
			
			System.out.println("Successfully synchronized, your graph is now available as " + graphName);
			// store the graph name for future use
			storeVariable("GRAPH_NAME", graphName);
			
			// store the directories used for the iGraph
			List<String> graphLocs = new Vector<String>();
			if(retrieveVariable(R_GRAQH_FOLDERS) != null) {
				graphLocs = (List<String>) retrieveVariable(R_GRAQH_FOLDERS);
			}
			graphLocs.add(wd);
			storeVariable(R_GRAQH_FOLDERS, graphLocs);
		} catch(Exception ex) {
			ex.printStackTrace();
			System.out.println("ERROR ::: Could not convert TinkerFrame into iGraph.\nPlease make sure iGraph package is installed.");
		} finally {
			// reset back to the original wd
			if(curWd != null) {
				eval("setwd(\"" + curWd + "\")");
			}
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
