package prerna.sablecc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

import au.com.bytecode.opencsv.CSVReader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.matching.DomainValues;
import prerna.algorithm.learning.matching.MatchingDB;
import prerna.cache.ICache;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.ds.util.CsvFileIterator;
import prerna.ds.util.IFileIterator;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.poi.main.HeadersException;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.poi.main.helper.XLFileHelper;
import prerna.rdf.main.ImportRDBMSProcessor;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

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
	 * These are the methods that we cannot make generic between the
	 * implementations of R
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

	protected abstract Object[][] getColumnCount(String frameName, String colName, boolean outputString, boolean top);
	
	protected abstract Object[][] getDescriptiveStats(String frameName, String colName, boolean outputString);

	protected abstract Object[][] getHistogram(String frameName, String column, int numBreaks, boolean print);

	protected abstract void performSplitColumn(String frameName, String columnName, String separator, String direction, boolean dropColumn, boolean frameReplace);

	protected abstract void performJoinColumns(String frameName, String newColumnName, String separator, String cols);

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////////// R Methods /////////////////////////

	/**
	 * Shift the dataframe into R with a default name
	 */
	public void synchronizeToR() {
		java.lang.System.setSecurityManager(curManager);
		if (dataframe instanceof TinkerFrame) {
			synchronizeGraphToR();
		} else if (dataframe instanceof H2Frame) {
			synchronizeGridToR();
		}
	}

	private static String getDefaultName() {
		// TODO: need to check variable names
		// make sure default name won't override
		return "df_" + counter++;
	}

	/**
	 * Shift the dataframe into R
	 * 
	 * @param rVarName
	 */
	public void synchronizeToR(String rVarName) {
		java.lang.System.setSecurityManager(curManager);
		if (dataframe instanceof TinkerFrame) {
			synchronizeGraphToR(rVarName);
		} else if (dataframe instanceof H2Frame) {
			synchronizeGridToR(rVarName);
		}
	}

	public void synchronizeFromR() {
		if (dataframe instanceof TinkerFrame) {
			String graphName = (String) retrieveVariable("GRAPH_NAME");
			synchronizeGraphFromR(graphName);
		} else if (dataframe instanceof H2Frame) {
			String frameName = (String) retrieveVariable("GRID_NAME");
			synchronizeGridFromR(frameName, true);
		}
	}

	/**
	 * Install a R package
	 * 
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
	 * 
	 * @param frameName
	 */
	protected void synchronizeGridToR(String rVarName) {
		synchronizeGridToR(rVarName, null);
	}

	/**
	 * Synchronize the grid to R
	 * 
	 * @param frameName
	 * @param cols
	 */
	private void synchronizeGridToR(String frameName, String cols) {
		H2Frame gridFrame = (H2Frame) dataframe;
		String tableName = gridFrame.getBuilder().getTableName();
		String url = gridFrame.getBuilder().connectFrame();
		url = url.replace("\\", "/");
		
		// note : do not use * since R will not preserve the column order
		StringBuilder selectors = new StringBuilder();
		String[] colSelectors = null;
		if (cols == null || cols.length() == 0) {
			colSelectors = gridFrame.getColumnHeaders();
		} else {
			colSelectors = cols.split(";");
		}

		for (int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
			selectors.append(colSelectors[selectIndex]);
			if (selectIndex + 1 < colSelectors.length) {
				selectors.append(", ");
			}
		}
		
		startR();
		
		// Don't sync via RJDBC if OS is Mac because we'll write to CSV and load into data.table to avoid rJava setup
		String OS = java.lang.System.getProperty("os.name").toLowerCase();
		if(OS.contains("mac")) {
			String outputLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/") + java.lang.System.getProperty("file.separator") + "R" 
					+ java.lang.System.getProperty("file.separator") + "Temp" + java.lang.System.getProperty("file.separator") + "output.csv";
			gridFrame.execQuery("CALL CSVWRITE('" + outputLocation + "', 'SELECT * FROM " + gridFrame.getTableName() + "', 'charset=UTF-8 fieldSeparator=, fieldDelimiter=');");
			eval("file <- '" + outputLocation + "';");
			eval(frameName + " <- read.csv(file);");
			File f = new File(outputLocation);
			f.delete();
		} else {
			initiateDriver(url, "sa");
			
			eval(frameName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName
					+ "')));");
		}
		eval("setDT(" + frameName + ")");
		
		// modify the headers to be what they used to be because the query
		// return everything in
		// all upper case which may not be accurate
		String[] currHeaders = getColNames(frameName, false);
		renameColumn(frameName, currHeaders, colSelectors, false);
		storeVariable("GRID_NAME", frameName);
		System.out.println("Completed synchronization as " + frameName);
	}

	/**
	 * Synchronize current H2Frame into a R Data Table Frame
	 * 
	 * @param rVarName
	 */
	protected void synchronizeGridToRDataTable(String rVarName) {
		if (dataframe instanceof H2Frame) {
			// if there is a current r serve session
			// use that for the frame so we have all the other variables
			RDataTable table = null;
			if (retrieveVariable(R_CONN) != null && retrieveVariable(R_PORT) != null) {
				table = new RDataTable(rVarName, (RConnection) retrieveVariable(R_CONN),
						(String) retrieveVariable(R_PORT));
			} else {
				// if we dont have a current r session
				// but when we create the table it makes one
				// store those variables so we end up using that
				table = new RDataTable(rVarName);
				if (table.getConnection() != null && table.getPort() != null) {
					storeVariable(R_CONN, table.getConnection());
					storeVariable(R_PORT, table.getPort());
				}
			}

			// should pass the user id down to the frame
			// important when we merge back the frame
			table.setUserId(dataframe.getUserId());

			H2Frame gridFrame = (H2Frame) dataframe;
			String tableName = gridFrame.getBuilder().getTableName();
			String url = gridFrame.getBuilder().connectFrame();
			url = url.replace("\\", "/");
			initiateDriver(url, "sa");

			// need to create a new data table
			// should properly merge the meta data
//			Map<String, Set<String>> edgeHash = gridFrame.getEdgeHash();
//			Map<String, String> dataTypeMap = new HashMap<String, String>();
//			for (String colName : edgeHash.keySet()) {
//				if (!dataTypeMap.containsKey(colName)) {
//					dataTypeMap.put(colName, Utility.convertDataTypeToString(gridFrame.getDataType(colName)));
//				}
//
//				Set<String> otherCols = edgeHash.get(colName);
//				for (String otherCol : otherCols) {
//					if (!dataTypeMap.containsKey(otherCol)) {
//						dataTypeMap.put(otherCol, Utility.convertDataTypeToString(gridFrame.getDataType(otherCol)));
//					}
//				}
//			}
//			table.mergeEdgeHash(edgeHash, dataTypeMap);
			
			table.setMetaData(gridFrame.getMetaData());
			
			StringBuilder selectors = new StringBuilder();
			String[] colSelectors = gridFrame.getColumnHeaders();
			for (int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
				selectors.append(colSelectors[selectIndex]);
				if (selectIndex + 1 < colSelectors.length) {
					selectors.append(", ");
				}
			}

			synchronizeGridToR(rVarName, null);
			
			this.dataframe = table;
			this.frameChanged = true;
		} else if(dataframe instanceof RDataTable){
			// ughhh... why are you calling this?
			// i will just change the r var name
			((RDataTable) dataframe).executeRScript(rVarName + " <- " + ((RDataTable) dataframe).getTableVarName());
			((RDataTable) dataframe).setTableVarName(rVarName);
		} else {
			throw new IllegalArgumentException("Frame must be of type H2");
		}
	}

	/**
	 * Create a H2Frame from an existing R data table
	 */
	protected void synchronizeGridFromR() {
		String frameName = (String) retrieveVariable("GRID_NAME");
		synchronizeGridFromR(frameName, true);
	}

	/**
	 * Synchronize a R data table into a H2Frame
	 * 
	 * @param frameName
	 * @param overrideExistingTable
	 */
	protected void synchronizeGridFromR(String frameName, boolean overrideExistingTable) {
		// get the necessary information from the r frame
		// to be able to add the data correctly

		// get the names and types
		String[] colNames = getColNames(frameName, false);
		// since R has less restrictions than we do regarding header names
		// we will clean the header names to match what the cleaning would be
		// when we load
		// in a file
		// note: the clean routine will only do something if the metadata has
		// changed
		// otherwise, the headers would already be good to go
		List<String> cleanColNames = new Vector<String>();
		HeadersException headerException = HeadersException.getInstance();
		for (int i = 0; i < colNames.length; i++) {
			String cleanHeader = headerException.recursivelyFixHeaders(colNames[i], cleanColNames);
			cleanColNames.add(cleanHeader);
		}
		colNames = cleanColNames.toArray(new String[] {});

		String[] colTypes = getColTypes(frameName, false);

		// need to create a data type map and a query struct
		QueryStruct qs = new QueryStruct();
		// TODO: REALLY NEED TO CONSOLIDATE THE STRING VS. METADATA TYPE
		// DATAMAPS
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		for (int i = 0; i < colNames.length; i++) {
			dataTypeMapStr.put(colNames[i], colTypes[i]);
			dataTypeMap.put(colNames[i], Utility.convertStringToDataType(colTypes[i]));
			qs.addSelector(colNames[i], null);
		}

		/*
		 * logic to determine where we are adding this data... 1) First, make
		 * sure the existing frame is a grid -> If it is not a grid, we already
		 * know we need to make a new h2frame 2) Second, if it is a grid, check
		 * the meta data and see if it has changed -> if it has changed, we need
		 * to make a new h2frame 3) Regardless of #2 -> user can decide what
		 * they want to create a new frame even if the meta data hasn't changed
		 */

		boolean frameIsH2 = false;
		String schemaName = null;
		String tableName = null;
		boolean determineNewFrameNeeded = false;
		boolean syncExistingRMetadata = false;
		
		// if we dont even have a h2frame currently, make a new one
		if (!(dataframe instanceof H2Frame)) {
			determineNewFrameNeeded = true;
			if(dataframe instanceof RDataTable && ((RDataTable) dataframe).getTableVarName().equals(frameName)) {
				syncExistingRMetadata = true;
			}
		} else {
			frameIsH2 = true;
			schemaName = ((H2Frame) dataframe).getSchema();
			tableName = ((H2Frame) dataframe).getTableName();

			// if we do have an h2frame, look at headers to figure
			// out if the metadata has changed

			String[] currHeaders = dataframe.getColumnHeaders();

			if (colNames.length != currHeaders.length) {
				determineNewFrameNeeded = true;
			} else {
				for (String currHeader : currHeaders) {
					if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(colNames, currHeader)) {
						determineNewFrameNeeded = true;
					}
				}
			}
		}

		H2Frame frameToUse = null;
		if (!overrideExistingTable || determineNewFrameNeeded) {
			frameToUse = new H2Frame();

			// set the correct schema in the new frame
			// drop the existing table
			if (frameIsH2) {
				frameToUse.setUserId(schemaName);
				((H2Frame) dataframe).dropTable();
			} else {
				// this is set when we set the original dataframe
				// within the reactor
				frameToUse.setUserId(this.userId);
			}
			
			// if we can use the existing metadata, use it
			if(syncExistingRMetadata) {
				frameToUse.setMetaData(this.dataframe.getMetaData());
			} else {
				// create a prim key one
				Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
				frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);
			}
			// override frame references & table name reference
			this.put("G", frameToUse);
			this.dataframe = frameToUse;
			this.frameChanged = true;
			tableName = frameToUse.getTableName();

		} else if (overrideExistingTable && frameIsH2) {
			frameToUse = ((H2Frame) dataframe);

			// can only enter here when we are overriding the existing H2Frame
			// drop any index if altering the existing frame
			Set<String> columnIndices = frameToUse.getColumnsWithIndexes();
			if (columnIndices != null) {
				for (String colName : columnIndices) {
					frameToUse.removeColumnIndex(colName);
				}
			}

			// drop all existing data
			frameToUse.deleteAllRows();
		}

		// we will make a temp file
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		eval("fwrite(" + frameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		CsvFileIterator dataIterator = CsvFileIterator.createInstance(IFileIterator.FILE_DATA_TYPE.META_DATA_ENUM, tempFileLocation, ',',
				qs, dataTypeMap, null);

		// keep track of in-mem vs on-disk frames
		int limitSizeInt = RdbmsFrameUtility.getLimitSize();
		if (dataIterator.numberRowsOverLimit(limitSizeInt)) {
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
	 * 
	 * @param frameName
	 * @param overrideExistingTable
	 */
	protected void synchronizeGridFromRDataTable(String frameName) {
		synchronizeGridFromR(frameName, true);
		// RDataTable rFrame = (RDataTable) dataframe;
		// Map<String, Set<String>> edgeHash = rFrame.getEdgeHash();
		//
		// // need to create a data type map and a query struct
		// QueryStruct qs = new QueryStruct();
		// // TODO: REALLY NEED TO CONSOLIDATE THE STRING VS. METADATA TYPE
		// DATAMAPS
		// Map<String, Set<String>> cleanEdgeHash = new HashMap<String,
		// Set<String>>();
		// Map<String, IMetaData.DATA_TYPES> dataTypeMap = new HashMap<String,
		// IMetaData.DATA_TYPES>();
		// Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		// for(String colName : edgeHash.keySet()) {
		// String cleanColName = colName.toUpperCase();
		// if(!dataTypeMap.containsKey(cleanColName)) {
		// DATA_TYPES type = rFrame.getDataType(colName);
		// dataTypeMap.put(cleanColName, type);
		// dataTypeMapStr.put(cleanColName,
		// Utility.convertDataTypeToString(type) );
		// qs.addSelector(cleanColName, null);
		// }
		//
		// Set<String> cleanOtherCols = new HashSet<String>();
		// Set<String> otherCols = edgeHash.get(colName);
		// for(String otherCol : otherCols) {
		// String cleanOtherCol = otherCol.toUpperCase();
		// if(!dataTypeMap.containsKey(cleanOtherCol)) {
		// DATA_TYPES type = rFrame.getDataType(colName);
		// dataTypeMap.put(cleanOtherCol, type );
		// dataTypeMapStr.put(cleanOtherCol,
		// Utility.convertDataTypeToString(type) );
		// qs.addSelector(cleanOtherCol, null);
		// }
		// cleanOtherCols.add(cleanOtherCol);
		// }
		//
		// cleanEdgeHash.put(cleanColName, cleanOtherCols);
		// }
		//
		// // we will make a temp file
		// String tempFileLocation =
		// DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) +
		// "\\" +
		// DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		// tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		// tempFileLocation = tempFileLocation.replace("\\", "/");
		// eval("fwrite(" + frameName + ", file='" + tempFileLocation + "')");
		//
		// H2Frame table = new H2Frame();
		// table.mergeEdgeHash(cleanEdgeHash, dataTypeMapStr);
		//
		// // iterate through file and insert values
		// FileIterator dataIterator =
		// FileIterator.createInstance(FILE_DATA_TYPE.META_DATA_ENUM,
		// tempFileLocation, ',', qs, dataTypeMap);
		// table.addRowsViaIterator(dataIterator, dataTypeMap);
		// dataIterator.deleteFile();
		//
		// System.out.println("Table Synchronized as " + table.getTableName());
		//
		// this.dataframe = table;
		// this.frameChanged = true;
	}

	protected void initiateDriver(String url, String username) {
		String driver = "org.h2.Driver";
		String jarLocation = "";
		if (retrieveVariable("H2DRIVER_PATH") == null) {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");
			;
			String jar = "h2-1.4.185.jar"; // TODO: create an enum of available
											// drivers and the necessary jar for
											// each
			jarLocation = workingDir + "/RDFGraphLib/" + jar;
		} else {
			jarLocation = (String) retrieveVariable("H2DRIVER_PATH");
		}
		LOGGER.info("Loading driver.. " + jarLocation);
		// line of R script that connects to H2Frame
		String script = "drv <- JDBC('" + driver + "', '" + jarLocation + "', identifier.quote='`');"
				+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', '')"; 
		runR(script);
	}

	/**
	 * Synchronize a CSV File into an R Data Table
	 * 
	 * @param fileName
	 * @param frameName
	 */
	protected void synchronizeCSVToR(String fileName, String frameName) {
		eval(frameName + " <- fread(\"" + fileName + "\")");
		System.out.println("Completed synchronization of CSV " + fileName);
	}

	protected String getColType(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
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
	 * 
	 * @param column
	 */
	protected Object[][] getColumnCount(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		return getColumnCount(frameName, colName);
	}

	protected Object[][] getColumnCount(String frameName, String colName) {
		return getColumnCount(frameName, colName, true);
	}

	/**
	 * Get the column count of a given column
	 * 
	 * @param column
	 */
	protected Object[][] getDescriptiveStats(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
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
	 * 
	 * @param newColName
	 */
	protected void addEmptyColumn(String newColName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		addEmptyColumn(frameName, newColName);
	}

	protected void addEmptyColumn(String frameName, String newColName) {
		String script = frameName + "$" + newColName + " <- \"\" ";
		eval(script);
		System.out.println("Successfully added column = " + newColName);
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	/**
	 * Add an empty column to later insert new values
	 * 
	 * @param newColName
	 */
	protected void changeColumnType(String colName, String newType) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		changeColumnType(frameName, colName, newType);
	}

	protected void changeColumnType(String frameName, String colName, String newType) {
		changeColumnType(frameName, colName, newType, "%Y/%m/%d");
	}

	protected void changeColumnType(String frameName, String colName, String newType, String dateFormat) {
		String script = null;
		if (newType.equalsIgnoreCase("string")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.character(" + colName + ")]";
			eval(script);
		} else if (newType.equalsIgnoreCase("factor")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.factor(" + colName + ")]";
			eval(script);
		} else if (newType.equalsIgnoreCase("number")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.numeric(" + colName + ")]";
			eval(script);
		} else if (newType.equalsIgnoreCase("date")) {
			// we have a different script to run if it is a str to date
			// conversion
			// or a date to new date format conversion
			String type = getColType(frameName, colName, false);
			String tempTable = Utility.getRandomString(6);
			if (type.equalsIgnoreCase("date")) {
				String formatString = ", format = '" + dateFormat + "'";
				script = tempTable + " <- format(" + frameName + "$" + colName + formatString + ")";
				eval(script);
				script = frameName + "$" + colName + " <- " + "as.Date(" + tempTable + formatString + ")";
				eval(script);
			} else {
				script = tempTable + " <- as.Date(" + frameName + "$" + colName + ", format='" + dateFormat + "')";
				eval(script);
				script = frameName + "$" + colName + " <- " + tempTable;
				eval(script);
			}
			// perform variable cleanup
			eval("rm(" + tempTable + ");");
			eval("gc();");
		}
		System.out.println("Successfully changed data type for column = " + colName);
		if (checkRTableModified(frameName)) {
			this.dataframe.getMetaData().storeDataType(colName, newType);
//			// TODO: should be able to change the data type dynamically!!!
//			// TODO: come back and fix this
//			recreateMetadata(frameName);
		}
	}

	/**
	 * Drop a column within the table
	 * 
	 * @param colName
	 */
	protected void dropRColumn(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		dropRColumn(frameName, colName);
	}

	protected void dropRColumn(String frameName, String colName) {
		if (checkRTableModified(frameName)) {
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
	 * 
	 * @param colName
	 * @param comparator
	 * @param values
	 */
	protected void dropRowsWhereColumnContainsValue(String colName, String comparator, Object values) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, values);
	}

	/**
	 * Filter out rows based on values in a given column
	 * 
	 * @param frameName
	 * @param colName
	 * @param comparator
	 * @param values
	 */
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, Object values) {
		// to account for misunderstandings between = and == for normal users
		if (comparator.trim().equals("=")) {
			comparator = " == ";
		}
		String frameExpression = frameName + "$" + colName;
		// determine the correct comparison to drop values from the frame
		// .... this is a bunch of casting...
		// also note that the string NULL is special to remove values that are
		// undefined within the frame
		StringBuilder script = new StringBuilder(frameName).append(" <- ").append(frameName).append("[!( ");
		String dataType = getColType(frameName, colName, false);

		// accommodate for factors cause they are annoying
		if (dataType.equals("factor")) {
			changeColumnType(frameName, colName, "STRING");
			dataType = "character";
		}

		if (values instanceof Object[]) {
			Object[] arr = (Object[]) values;
			Object val = arr[0];
			if (dataType.equalsIgnoreCase("character")) {
				if (val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
					script.append("is.na(").append(frameExpression).append(") ");
				} else {
					if(comparator.equals("like")) {
						script.append("like(").append(frameExpression).append(",").append("\"").append(val).append("\")");
					} else {
						script.append(frameExpression).append(comparator).append("\"").append(val).append("\"");
					}
				}
			} else {
				script.append(comparator).append(val);
			}
			for (int i = 1; i < arr.length; i++) {
				val = arr[i];
				if (dataType.equalsIgnoreCase("character")) {
					if (val.toString().equalsIgnoreCase("NULL") || val.toString().equalsIgnoreCase("NA")) {
						script.append(" | is.na(").append(frameExpression).append(") ");
					} else {
						if(comparator.equals("like")) {
							script.append(" | ").append("like(").append(frameExpression).append(",").append("\"").append(val).append("\")");
						} else {
							script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val).append("\"");						}
					}
				} else {
					script.append(" | ").append(frameExpression).append(comparator).append(val);
				}
			}
		} else if (values instanceof Double[]) {
			Double[] arr = (Double[]) values;
			Double val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for (int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if (values instanceof Integer[]) {
			Integer[] arr = (Integer[]) values;
			Integer val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for (int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if (values instanceof double[]) {
			double[] arr = (double[]) values;
			double val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for (int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else if (values instanceof int[]) {
			int[] arr = (int[]) values;
			int val = arr[0];
			script.append(frameExpression).append(comparator).append(val);
			for (int i = 1; i < arr.length; i++) {
				val = arr[i];
				script.append(" | ").append(frameExpression).append(comparator).append(val);
			}
		} else {
			if (dataType.equalsIgnoreCase("character")) {
				if (values.toString().equalsIgnoreCase("NULL") || values.toString().equalsIgnoreCase("NA")) {
					script.append("is.na(").append(frameExpression).append(") ");
				} else {
					if(comparator.equals("like")) {
						script.append("like(").append(frameExpression).append(",").append("\"").append(values).append("\")");
					} else {
						script.append(frameExpression).append(comparator).append("\"").append(values).append("\"");
					}
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
		String frameName = (String) retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, value);
	}

	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, int value) {
		// to account for misunderstandings between = and == for normal users
		if (comparator.trim().equals("=")) {
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
		String frameName = (String) retrieveVariable("GRID_NAME");
		dropRowsWhereColumnContainsValue(frameName, colName, comparator, value);
	}

	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator, double value) {
		// to account for misunderstandings between = and == for normal users
		if (comparator.trim().equals("=")) {
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
	 * Create a new column by counting the presence of a string within another
	 * column
	 * 
	 * @param newColName
	 * @param countColName
	 * @param strToCount
	 */
	protected void insertStrCountColumn(String newColName, String countColName, String strToCount) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		insertStrCountColumn(frameName, newColName, countColName, strToCount);
	}

	protected void insertStrCountColumn(String newColName, String countColName, int valToCount) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		insertStrCountColumn(frameName, newColName, countColName, valToCount + "");
	}

	protected void insertStrCountColumn(String newColName, String countColName, double valToCount) {
		String frameName = (String) retrieveVariable("GRID_NAME");
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
		String script = frameName + "$" + newColName + " <- str_count(" + frameName + "$" + countColName + ", \""
				+ strToCount + "\")";
		eval(script);
		System.out.println("Added new column = " + newColName);
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	/**
	 * Turn a string to lower case
	 * 
	 * @param colName
	 */
	protected void toLowerCase(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		toLowerCase(frameName, colName);
	}

	protected void toLowerCase(String frameName, String colName) {
//		String[] colSplit = colName.split(",");
//		for(String col : colSplit) {
//			if(col == null || col.isEmpty()) {
//				continue;
//			}
			String script = frameName + "$" + colName + " <- tolower(" + frameName + "$" + colName + ")";
			eval(script);
//		}
		checkRTableModified(frameName);
	}

	/**
	 * Turn a string to lower case
	 * 
	 * @param colName
	 */
	protected void toUpperCase(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		toUpperCase(frameName, colName);
	}

	protected void toUpperCase(String frameName, String colName) {
//		String[] colSplit = colName.split(",");
//		for(String col : colSplit) {
//			if(col == null || col.isEmpty()) {
//				continue;
//			}
			String script = frameName + "$" + colName + " <- toupper(" + frameName + "$" + colName + ")";
			eval(script);
//		}
		checkRTableModified(frameName);
	}

	/**
	 * Turn a string to lower case
	 * 
	 * @param colName
	 */
	protected void trim(String colName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		trim(frameName, colName);
	}

	protected void trim(String frameName, String colName) {
//		String[] colSplit = colName.split(",");
//		for(String col : colSplit) {
//			if(col == null || col.isEmpty()) {
//				continue;
//			}
			String script = frameName + "$" + colName + " <- str_trim(" + frameName + "$" + colName + ")";
			eval(script);
//		}
		checkRTableModified(frameName);
	}

	/**
	 * Replace a column value with a new value
	 * 
	 * @param columnName
	 * @param curValue
	 * @param newValue
	 */
	protected void replaceColumnValue(String columnName, String curValue, String newValue) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		replaceColumnValue(frameName, columnName, curValue, newValue);
	}

	protected void replaceColumnValue(String frameName, String columnName, String curValue, String newValue) {
		// replace the column value for a particular column
		// dt[PY == "hello", PY := "D"] replaces a column conditionally based on
		// the value
		// need to get the type of this
		try {
			String condition = " ,";
			String dataType = getColType(columnName);
			String quote = "";
			if (dataType.contains("character")) {
				quote = "\"";
			} else if (dataType.equals("factor")) {
				changeColumnType(frameName, columnName, "STRING");
				quote = "\"";
			}
			if (curValue.equalsIgnoreCase("null") || curValue.equalsIgnoreCase("NA")) {
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
	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue,
			String conditionalColName, String comparator, Object conditionalColValue) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName,
				comparator, conditionalColValue);
	}

	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue,
			String conditionalColName, String comparator, double conditionalColValue) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColValue, conditionalColName, comparator,
				conditionalColValue + "");
	}

	protected void updateRowValuesWhereColumnContainsValue(String updateColName, Object updateColValue,
			String conditionalColName, String comparator, int conditionalColValue) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		updateRowValuesWhereColumnContainsValue(frameName, updateColValue, conditionalColName, comparator,
				conditionalColValue + "");
	}

	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName,
			Object updateColValue, String conditionalColName, String comparator, double conditionalColValue) {
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName,
				comparator, conditionalColValue + "");
	}

	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName,
			Object updateColValue, String conditionalColName, String comparator, int conditionalColValue) {
		updateRowValuesWhereColumnContainsValue(frameName, updateColName, updateColValue, conditionalColName,
				comparator, conditionalColValue + "");
	}

	protected void updateRowValuesWhereColumnContainsValue(String frameName, String updateColName,
			Object updateColValue, String conditionalColName, String comparator, Object conditionalColValue) {
		// update values based on other columns
		// dt$updateColName[dt$conditionalColName == "conditionalColValue] <-
		// updateColValue
		// need to get the types of this
		try {
			if (comparator.trim().equals("=")) {
				comparator = "==";
			}
			comparator = " " + comparator + " ";

			String updateDataType = getColType(updateColName);
			String updateQuote = "";
			if (updateDataType.contains("character") || updateDataType.contains("factor")) {
				updateQuote = "\"";
			}

			String conditionColDataType = getColType(conditionalColName);
			String conditionColQuote = "";
			if (conditionColDataType.contains("character")) {
				conditionColQuote = "\"";
			}

			String script = frameName + "$" + updateColName + "[" + frameName + "$" + conditionalColName + comparator
					+ conditionColQuote + conditionalColValue + conditionColQuote + "] <- " + updateQuote
					+ updateColValue + updateQuote;
			eval(script);
			System.out.println("Done updating column " + updateColName + " where " + conditionalColName + comparator
					+ conditionalColValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkRTableModified(frameName);
	}

	/**
	 * Regex replace a column value with a new value
	 * 
	 * @param columnName
	 * @param curValue
	 * @param newValue
	 */
	protected void regexReplaceColumnValue(String columnName, String regex, String newValue) {
		String frameName = (String) retrieveVariable("GRID_NAME");
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
			if (dataType.contains("character") || dataType.contains("factor")) {
				quote = "\"";
			}
			script += "gsub(" + quote + regex + quote + "," + quote + newValue + quote + ", " + colScript + ")";
			eval(script);
			System.out.println(
					"Done replacing value with regex = \"" + regex + "\" with new value = \"" + newValue + "\"");
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkRTableModified(frameName);
	}

	protected void splitColumn(String columnName, String separator) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		splitColumn(frameName, columnName, separator, "wide", false, true);
	}

	protected void splitColumn(String frameName, String columnName, String separator) {
		splitColumn(frameName, columnName, separator, "wide", false, true);
	}
	
	protected void splitColumn(String frameName, String columnName, String separator, String direction) {
		splitColumn(frameName, columnName, separator, direction, false, true);
	}

	protected void splitColumn(String frameName, String columnName, String separator, String direction, boolean dropColumn, boolean frameReplace) {
		performSplitColumn(frameName, columnName, separator, direction, false, true);
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	protected void joinColumns(String newColumnName, String separator, String cols) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		joinColumns(frameName, newColumnName, separator, cols);
	}

	protected void joinColumns(String frameName, String newColumnName, String separator, String cols) {
		performJoinColumns(frameName, newColumnName, separator, cols);
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	protected void transpose() {
		String frameName = (String) retrieveVariable("GRID_NAME");
		transpose(frameName);
	}

	protected void transpose(String frameName) {
		String script = frameName + " <- " + frameName + "[, data.table(t(.SD), keep.rownames=TRUE)]";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully transposed data table into existing frame");
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	protected void transpose(String frameName, String transposeFrameName) {
		String script = transposeFrameName + " <- " + frameName + "[, data.table(t(.SD), keep.rownames=TRUE)]";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully transposed data table into new frame " + transposeFrameName);
		if (checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}

	protected void unpivot() {
		String frameName = (String) retrieveVariable("GRID_NAME");
		unpivot(frameName, null, true);
	}

	protected void unpivot(String frameName, String cols, boolean replace) {
		// makes the columns and converts them into rows
		// melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		startR();
		String concatString = "";
		String tempName = Utility.getRandomString(8);

		if (cols != null && cols.length() > 0) {
			String[] columnsToPivot = cols.split(";");
			concatString = ", measure.vars = c(";
			for (int colIndex = 0; colIndex < columnsToPivot.length; colIndex++) {
				concatString = concatString + "\"" + columnsToPivot[colIndex] + "\"";
				if (colIndex + 1 < columnsToPivot.length)
					concatString = concatString + ", ";
			}
			concatString = concatString + ")";
		}
		String script = tempName + "<- melt(" + frameName + concatString + ");";
		// run the first script to unpivot into the temp frame
		eval(script);
		// if we are to replace the existing frame
		if (replace) {
			script = frameName + " <- " + tempName;
			eval(script);
			if (checkRTableModified(frameName)) {
				recreateMetadata(frameName);
			}
		}
		System.out.println("Done unpivoting...");
	}

	protected void pivot(String columnToPivot, String valueToPivot, String cols) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		pivot(frameName, true, columnToPivot, valueToPivot, cols, null);
	}

	protected void pivot(String frameName, boolean replace, String columnToPivot, String valueToPivot, String cols) {
		pivot(frameName, true, columnToPivot, valueToPivot, cols, null);
	}

	protected void pivot(String frameName, boolean replace, String columnToPivot, String valueToPivot, String cols, String aggregateFunction) {
		// makes the columns and converts them into rows
		// dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		startR();
		String newFrame = Utility.getRandomString(8);

		String keepString = "";
		if (cols != null && cols.length() > 0) {
			String[] columnsToKeep = cols.split(";");
			// with the portion of code to ignore if the user passes in the 
			// col to pivot or value to pivot in the selected columns
			// we need to account for this so we dont end the keepString with " + "
			int size = columnsToKeep.length;
			keepString = ", formula = ";
			for (int colIndex = 0; colIndex < size; colIndex++) {
				String newKeepString = columnsToKeep[colIndex];
				if(newKeepString.equals(columnToPivot) || newKeepString.equals(valueToPivot)) {
					continue;
				}
				keepString = keepString + newKeepString;
				if (colIndex + 1 < size) {
					keepString = keepString + " + ";
				}
			}
			
			// with the portion of code to ignore if the user passes in the 
			// col to pivot or value to pivot in the selected columns
			// we need to account for this so we dont end the keepString with " + "
			if(keepString.endsWith(" + ")) {
				keepString = keepString.substring(0, keepString.length() - 3);
			}
			keepString = keepString + " ~ " + columnToPivot + ", value.var=\"" + valueToPivot + "\"";
		}

		String aggregateString = "";
		if (aggregateFunction != null && aggregateFunction.length() > 0) {
			aggregateString = ", fun.aggregate = " + aggregateFunction + " , na.rm = TRUE";
		}
		String script = newFrame + " <- dcast(" + frameName + keepString + aggregateString + ");";
		eval(script);
		script = newFrame + " <- as.data.table(" + newFrame + ");";
		eval(script);
		if (replace) {
			script = frameName + " <- " + newFrame;
			eval(script);
			if (checkRTableModified(frameName)) {
				recreateMetadata(frameName);
			}
		}
		System.out.println("Done pivoting...");
	}

	protected void recreateMetadata(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColNames(frameName, false);
		String[] colTypes = getColTypes(frameName, false);

		// create the data type map and the edge hash
		Map<String, String> dataTypeMap = new Hashtable<String, String>();
		for (int i = 0; i < colNames.length; i++) {
			dataTypeMap.put(colNames[i], Utility.getCleanDataType(colTypes[i]));
		}
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);

		// create a new table using the correct variables
		// to get into this point
		// we know there is an existing r table
		// so if there is no r_conn + port variables
		// it must be JRI
		RDataTable newTable = null;
		if (retrieveVariable(R_CONN) != null && retrieveVariable(R_PORT) != null) {
			newTable = new RDataTable(frameName, (RConnection) retrieveVariable(R_CONN), (String) retrieveVariable(R_PORT));
		} else {
			newTable = new RDataTable(frameName);
		}
		newTable.mergeEdgeHash(edgeHash, dataTypeMap);

		int currDataId = this.dataframe.getDataId();
		for (int i = 0; i <= currDataId; i++) {
			newTable.updateDataId();
		}

		this.dataframe = newTable;
		this.frameChanged = true;
	}

	protected void renameColumn(String curColName, String newColName) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		renameColumn(frameName, curColName, newColName);
	}

	protected void renameColumn(String frameName, String curColName, String newColName) {
		String validNewHeader = getCleanNewHeader(frameName, newColName);
		String script = "names(" + frameName + ")[names(" + frameName + ") == \"" + curColName + "\"] = \"" + validNewHeader
				+ "\"";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully modified name = " + curColName + " to now be " + validNewHeader);
		if (checkRTableModified(frameName)) {
			this.dataframe.modifyColumnName(curColName, validNewHeader);
		}
	}
	
	private String getCleanNewHeader(String frameName, String newColName) {
		// make the new column name valid
		HeadersException headerChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColNames(frameName);
		String validNewHeader = headerChecker.recursivelyFixHeaders(newColName, currentColumnNames);
		return validNewHeader;
	}

	protected void renameColumn(String[] oldNames, String[] newColNames) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		renameColumn(frameName, oldNames, newColNames);
	}

	protected void renameColumn(String frameName, String[] oldNames, String[] newColNames) {
		renameColumn(frameName, oldNames, newColNames, true);
	}

	protected void renameColumn(String frameName, String[] oldNames, String[] newNames, boolean print) {
		int size = oldNames.length;
		if (size != newNames.length) {
			throw new IllegalArgumentException("Names arrays do not match in length");
		}
		StringBuilder oldC = new StringBuilder("c(");
		int i = 0;
		oldC.append("'").append(oldNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			oldC.append(", '").append(oldNames[i]).append("'");
		}
		oldC.append(")");

		StringBuilder newC = new StringBuilder("c(");
		i = 0;
		newC.append("'").append(newNames[i]).append("'");
		i++;
		for (; i < size; i++) {
			newC.append(", '").append(newNames[i]).append("'");
		}
		newC.append(")");

		String script = "setnames(" + frameName + ", old = " + oldC + ", new = " + newC + ")";
		eval(script);

		if (print) {
			System.out.println("Running script : " + script);
			System.out.println("Successfully modified old names = " + Arrays.toString(oldNames) + " to new names "
					+ Arrays.toString(newNames));
		}
		if (checkRTableModified(frameName)) {
			for (i = 0; i < size; i++) {
				this.dataframe.modifyColumnName(oldNames[i], newNames[i]);
			}
		}
	}
	
	/**
	 * Modify the specific cell value in the data frame
	 * 
	 * @param colName
	 * @param rowNum
	 * @param newVal
	 */
	protected void modifyCellValues(String colName, int rowNum, Object newVal) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}

	protected void modifyCellValues(String colName, int rowNum, int newVal) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}

	protected void modifyCellValues(String colName, int rowNum, double newVal) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		modifyCellValues(frameName, colName, rowNum, newVal);
	}

	protected void modifyCellValues(String frameName, String colName, int rowNum, Object newVal) {
		String type = getColType(frameName, colName, false);
		if (type.contains("character")) {
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
		if (type.contains("character")) {
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
		if (type.contains("character")) {
			value = "\"" + newVal + "\"";
		}
		String script = frameName + "[" + rowNum + "]$" + colName + " <- " + value;
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}

	/**
	 * If we order the data, we need to maintain that structure within the
	 * entire grid If we are to actually be able to replace values based on
	 * index
	 * 
	 * @param colName
	 * @param orderDirection
	 */
	protected void sortData(String colName, String orderDirection) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		sortData(frameName, colName, orderDirection);
	}

	protected void sortData(String frameName, String colName, String orderDirection) {
		String script = null;
		if (orderDirection == null || orderDirection.equalsIgnoreCase("asc")) {
			script = frameName + " <- " + frameName + "[order(rank(" + colName + "))]";
		} else if (orderDirection.equalsIgnoreCase("desc")) {
			script = frameName + " <- " + frameName + "[order(-rank(" + colName + "))]";
		}
		System.out.println("Running script " + script);
		eval(script);
		checkRTableModified(frameName);
	}

	/**
	 * Insert data at a given index into the frame
	 * 
	 * @param index
	 * @param values
	 */
	protected void insertDataAtIndex(int index, Object[] values) {
		String frameName = (String) retrieveVariable("GRID_NAME");
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
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				listScript.append(", ");
			}
			listScript.append(names[i]).append("=");
			if (types[i].equalsIgnoreCase("character")) {
				listScript.append("\"").append(values[i]).append("\"");
			} else {
				listScript.append(values[i]);
			}
		}
		listScript.append(")");
		eval(listScript.toString());

		String script = null;
		int totalRows = getNumRows(frameName);
		if (index == 1) {
			script = frameName + " <- rbindlist(list( " + listName + ", " + frameName + " ))";
		} else if (index == (totalRows + 1)) {
			script = frameName + " <- rbindlist(list( " + frameName + ", " + listName + " ))";
		} else {
			// ugh... somewhere in the middle
			script = frameName + " <- rbindlist(list(" + frameName + "[1:" + (index - 1) + ",] , " + listName + " , "
					+ frameName + "[" + index + ":" + totalRows + ",] ))";
		}
		eval(script);
		System.out.println("Running script :\n" + listScript + "\n" + script);

		checkRTableModified(frameName);
	}

	protected boolean checkRTableModified(String frameName) {
		if (this.dataframe instanceof RDataTable) {
			String tableVarName = ((RDataTable) this.dataframe).getTableVarName();
			if (frameName.equals(tableVarName)) {
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
			if (ret == null) {
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
			if (retrieveVariable(R_GRAQH_FOLDERS) != null) {
				graphLocs = (List<String>) retrieveVariable(R_GRAQH_FOLDERS);
			}
			graphLocs.add(wd);
			storeVariable(R_GRAQH_FOLDERS, graphLocs);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(
					"ERROR ::: Could not convert TinkerFrame into iGraph.\nPlease make sure iGraph package is installed.");
		} finally {
			// reset back to the original wd
			if (curWd != null) {
				eval("setwd(\"" + curWd + "\")");
			}
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	/**
	 * Synchronize graph from iGraph
	 * 
	 * @param graphName
	 */
	private void synchronizeGraphFromR(String graphName) {
		System.out.println("ERROR ::: Have not implemented synchronizeGraphFromR yet...");

		// get the attributes
		// and then synchronize all the different properties
		// vertex_attr_names
		// String names = "";
		// RConnection con = (RConnection)startR();
		//
		// // get all the attributes first
		// try {
		// String [] strings = con.eval("vertex_attr_names(" + graphName +
		// ")").asStrings();
		// // the question is do I get everything here and set tinker
		// // or for each get it and so I dont look up tinker many times ?!
		//
		// // now I need to get each of this string and then synchronize
		// } catch (RserveException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (REXPMismatchException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	/**
	 * Remove all nodes of a specific type and with a specific value
	 * 
	 * @param type
	 * @param data
	 */
	protected void removeNode(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if (dataframe instanceof TinkerFrame) {
			List<Object> removeList = new Vector<Object>();
			removeList.add(data);
			((TinkerFrame) dataframe).remove(type, removeList);
			String output = "Removed nodes for  " + data + " with values " + removeList;
			System.out.println(output);
			dataframe.updateDataId();
			removeNodeFromR(type, removeList);
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	/**
	 * Delete nodes from R iGraph
	 * 
	 * @param type
	 * @param nodeList
	 */
	protected void removeNodeFromR(String type, List<Object> nodeList) {
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		if (graphName == null) {
			// we will not have a graph name if the graph has not been
			// synchronized to R
			return;
		}
		for (int nodeIndex = 0; nodeIndex < nodeList.size(); nodeIndex++) {
			String name = type + ":" + nodeList.get(nodeIndex);
			try {
				java.lang.System.out.println("Deleting node = " + name);
				// eval is abstract and is determined by the specific R
				// implementation
				eval(graphName + " <- delete_vertices(" + graphName + ", V(" + graphName + ")[vertex_attr(" + graphName
						+ ", \"" + TinkerFrame.TINKER_ID + "\") == \"" + name + "\"])");
			} catch (Exception ex) {
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
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		if (graphName == null) {
			System.out.println("ERROR ::: No graph has been synchronized to R");
			return;
		}
		startR();
		try {
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			eval("clus <- " + clusterRoutine + "(" + graphName + ")");
			System.out.println("\n No. Of Components :");
			runR("clus$no");
			System.out.println("\n Component Sizes :");
			runR("clus$csize");
			colorClusters();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Perform cluster_walktrap routine on iGraph
	 */
	protected void walkInfo() {
		String graphName = (String) retrieveVariable("GRAPH_NAME");

		Rengine retEngine = (Rengine) startR();
		String clusters = "Component Information  \n";
		try {
			// set the clusters
			storeVariable("CLUSTER_NAME", "clus");
			retEngine.eval("clus <- cluster_walktrap(" + graphName + ", membership=TRUE)");
			clusters = clusters + "Completed Walktrap";
			System.out.println(clusters);
			colorClusters();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Color tinker nodes based on iGrpah values
	 */
	protected void colorClusters() {
		String clusterName = (String) retrieveVariable("CLUSTER_NAME");
		colorClusters(clusterName);
	}

	/**
	 * Serialize the TinkerGraph in GraphML format
	 * 
	 * @param directory
	 * @return
	 */
	public String writeGraph(String directory) {
		String absoluteFileName = null;
		if (dataframe instanceof TinkerFrame) {
			final Graph graph = ((TinkerFrame) dataframe).g;
			absoluteFileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
			String fileName = directory + "/" + absoluteFileName;
			OutputStream os = null;
			try {
				os = new FileOutputStream(fileName);
				graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (os != null) {
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
	 * Run a layout in iGraph and store back into tinker objects Possible
	 * values: Fruchterman - layout_with_fr KK - layout_with_kk sugiyama -
	 * layout_with_sugiyama layout_as_tree layout_as_star layout.auto
	 * http://igraph.org/r/doc/layout_with_fr.html
	 * 
	 * @param layout
	 */
	public void doLayout(String layout) {
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		// the color is saved as color
		try {
			eval("xy_layout <- " + layout + "(" + graphName + ")");
			synchronizeXY("xy_layout");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Identifies matching concepts for federation from the semicolon-delimited
	 * list of engines.
	 * 
	 * @param engines
	 *            The semicolon-delimited string of engine names
	 * @param candidateThreshold
	 *            Used during LSH hashing to set the number of minhash functions
	 *            and bands
	 * @param similarityThreshold
	 *            Only consider matches above this threshold
	 * @param instancesThreshold
	 *            Only consider concepts or properties with a number of unique
	 *            instances above this threshold
	 * @param compareProperties
	 *            Whether or not to consider properties as well as concepts
	 * @param refresh
	 *            Whether or not to refresh the corpus
	 */
	public void runCompatibilitySearch(String[] engines, double candidateThreshold, double similarityThreshold,
			int instancesThreshold, boolean compareProperties, boolean refresh) {

		String metadataFile = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + Constants.R_MATCHING_FOLDER
				+ "\\" + Constants.R_TEMP_FOLDER + "\\instanceCount.csv";
		metadataFile = metadataFile.replace("\\", "/");
		HashMap<String, String> allProperties = new HashMap<String, String>();

		if (engines.length < 2) {
			engines = new String[] { engines[0], engines[0] };
		}
		// Refresh the corpus
		if (refresh) {
			DomainValues dv = new DomainValues();
			try {
				String outputFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
						+ Constants.R_MATCHING_FOLDER + "\\" + Constants.R_TEMP_FOLDER + "\\"
						+ Constants.R_MATCHING_REPO_FOLDER;

				// Wipe out the old files
				FileUtils.cleanDirectory(new File(outputFolder));
				for (String engineName : engines) {
					IEngine engine = (IEngine) Utility.getEngine(engineName);
					HashMap<String, String> properties = dv.exportInstanceValues(engine, outputFolder,
							compareProperties, instancesThreshold);
					allProperties.putAll(properties);
					dv.exportRelationInstanceValues(engine, outputFolder, instancesThreshold);
				}
				try {
					ArrayList<String> rowValues = new ArrayList<String>();
					String row = "";
					for (String key : allProperties.keySet()) {
						String propertyValue = allProperties.get(key);
						row = key + "," + propertyValue;
						rowValues.add(row);
					}

					FileWriter fw = new FileWriter(metadataFile, false);
					// headers
					fw.write("sourceFileName, totalInstanceCount, hasProperties");
					for (String rowData : rowValues) {
						fw.write("\n" + rowData);
					}
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			} catch (IOException e) {
				LOGGER.error("Failed to refresh corpus");
				e.printStackTrace();
			}
		}

		// base folder Semoss/R/Matching
		String baseMatchingFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_MATCHING_FOLDER;
		baseMatchingFolder = baseMatchingFolder.replace("\\", "/");

		// Grab the corpus directory
		String corpusDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\"
				+ Constants.R_MATCHING_REPO_FOLDER;
		corpusDirectory = corpusDirectory.replace("\\", "/");

		// Grab the csv directory Semoss/R/Matching/Temp/rdf
		String baseRDFDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdf";
		baseRDFDirectory = baseRDFDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdf/MatchingCsvs
		String rdfCsvDirectory = baseRDFDirectory + "\\" + Constants.R_MATCHING_CSVS_FOLDER;
		rdfCsvDirectory = rdfCsvDirectory.replace("\\", "/");

		// Grab the prop directory Semoss/R/Matching/Temp/rdf/MatchingProp
		String rdfPropDirectory = baseRDFDirectory + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_MATCHING_PROP_FOLDER;
		rdfPropDirectory = rdfPropDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdbms
		String rdbmsDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdbms";
		rdbmsDirectory = rdbmsDirectory.replace("\\", "/");

		// Set the number of minhash functions and the number of bands
		// nMinhash should be divisible by nBands, and the greater the nBands
		// the slower the performance of the algorithm
		// These values are meant to optimize the balance between speed and the
		// probability of a match
		int nMinhash;
		int nBands;
		if (candidateThreshold <= 0.03) {
			nMinhash = 3640;
			nBands = 1820;
		} else if (candidateThreshold <= 0.05) {
			nMinhash = 1340;
			nBands = 670;
		} else if (candidateThreshold <= 0.1) {
			nMinhash = 400;
			nBands = 200;
		} else if (candidateThreshold <= 0.2) {
			nMinhash = 200;
			nBands = 100;
		} else if (candidateThreshold <= 0.4) {
			nMinhash = 210;
			nBands = 70;
		} else if (candidateThreshold <= 0.5) {
			nMinhash = 200;
			nBands = 50;
		} else {
			nMinhash = 200;
			nBands = 40;
		}

		// Parameters for R script
		String rFrameName = "this.dt.name.is.reserved.for.semantic.matching";

		// Grab the utility script
		String utilityScriptPath = baseMatchingFolder + "\\" + "matching.R";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		// TODO add this library to the list when starting R
		// This is also called in the function,
		// but by calling it here we can see if the user doesn't have the
		// package
		runR("library(textreuse)");

		// Source the LSH function from the utility script
		runR("source(\"" + utilityScriptPath + "\");");

		// Run locality sensitive hashing to generate matches
		runR(rFrameName + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + corpusDirectory + "\", " + nMinhash + ", "
				+ nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
				+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", \"" + rdfCsvDirectory + "\", \""
				+ rdbmsDirectory + "\", \"" + metadataFile + "\")");

		// Synchronize from R used to display resulting frame from R
		// storeVariable("GRID_NAME", rFrameName);
		// synchronizeFromR();

		String matchingDbName = MatchingDB.MATCHING_RDBMS_DB;

		// Delete previous matching database
		IEngine matchingEngine = matchingEngine = Utility.getEngine(matchingDbName);
		if (matchingEngine != null) {
			matchingEngine.deleteDB();
			matchingEngine = null;
		}
	

		// Only add to the engine if it is null
		if (matchingEngine == null) {
			MatchingDB db = new MatchingDB(getBaseFolder());
			// rdbms db
			String matchingDBType = ImportOptions.DB_TYPE.RDBMS.toString();
			db.saveDB(matchingDBType);
			
			// Clean directory
			// TODO clean the directory even when there is an error
			// try {
			// FileUtils.cleanDirectory(new File(corpusDirectory));
			// } catch (IOException e) {
			// System.out.println("Unable to clean directory");
			// e.printStackTrace();
			// }
		}
		
		// create data for heat map
		matchingEngine = Utility.getEngine(matchingDbName);
		H2Frame frame = new H2Frame();
		String[] frameNames = new String[] { "source_database", "target_database", "similarity_ratio" };
		String[] frameTypes = new String[] { "STRING", "STRING", "NUMBER" };

		QueryStruct qs = new QueryStruct();
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		for (int i = 0; i < frameNames.length; i++) {
			dataTypeMapStr.put(frameNames[i], frameTypes[i]);
			dataTypeMap.put(frameNames[i], Utility.convertStringToDataType(frameTypes[i]));
			qs.addSelector(frameNames[i], null);
		}
		frame.setUserId(this.userId);

		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(frameNames);
		frame.mergeEdgeHash(edgeHash, dataTypeMapStr);

		ArrayList<String> sourceEngines = MatchingDB.getSourceDatabases();

		// get match id count for source - target db
		HashMap<String, HashSet<String>> matchIDHash = new HashMap<>();
		if (matchingEngine != null) {
			String query = "SELECT match_id, source_database, target_database";
			query += " FROM match_id WHERE ";

			// add engines to query
			query += "source_database IN ( ";
			for (int i = 0; i < sourceEngines.size(); i++) {
				query += "'" + sourceEngines.get(i) + "'";
				if (i < sourceEngines.size() - 1) {
					query += ",";
				}
			}
			query += "); ";

			Map<String, Object> values = (Map<String, Object>) matchingEngine.execQuery(query);
			ResultSet rs = (ResultSet) values.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			try {
				while (rs.next()) {
					String matchID = rs.getString(1);
					// clean itemConcept
					String sourceDB = rs.getString(2);
					String targetDB = rs.getString(3);
					String relationshipRegEx = "%{3}";
					Pattern pattern = Pattern.compile(relationshipRegEx);
					Matcher matcher = pattern.matcher(matchID);
					matchID = matchID.replaceFirst("%", "@");
					String hashKey = sourceDB + "@" + targetDB;
					HashSet<String> tempHash = new HashSet<>();
					if (matcher.find()) {
						String[] temp = matchID.split("@");
						String source = temp[0];
						String target = temp[1];
						String leftSide = temp[0];
						String rightSide = temp[1];
						if (source.contains("%{3}")) {
							String[] sourceRelationship = source.split("%{3}");
							leftSide = sourceRelationship[1];
						}
						if (target.contains("%%%")) {
							String[] targetRelationship = target.split("%%%");
							rightSide = targetRelationship[1];
						}
						matchID = leftSide + "@" + rightSide;
						matchID = matchID.replaceAll(".", "");

						if (matchID.length() > 0) {
							if (matchIDHash.containsKey(hashKey)) {
								tempHash = matchIDHash.get(hashKey);
							}
							// Split matchID to get left side match
							tempHash.add(matchID.split("@")[0]);
							matchIDHash.put(hashKey, tempHash);
						}
					} else {
						if (matchID.length() > 0) {
							if (matchIDHash.containsKey(hashKey)) {
								tempHash = matchIDHash.get(hashKey);
							}
							// Split matchID to get left side match
							tempHash.add(matchID.split("@")[0]);
							matchIDHash.put(hashKey, tempHash);
						}
					}

				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}

		// write matchID for sourceDB to itself
		for (String eng : sourceEngines) {
			matchIDHash.put(eng + "@" + eng, new HashSet<>());
		}

		for (String sourceTargetKey : matchIDHash.keySet()) {
			HashSet<String> ids = matchIDHash.get(sourceTargetKey);
			String[] dbs = sourceTargetKey.split("@");
			String sourceDB = dbs[0];
			String targetDB = dbs[1];
			String totalColumnCount = allProperties.get(sourceDB);
			double matchColumnCount = ids.size();
			double ratio = matchColumnCount / Integer.parseInt(totalColumnCount);

			// source engine matching to itself
			if (sourceDB.equals(targetDB)) {
				ratio = 1;
			}
			// System.out.println("sourceDB " + sourceDB);
			// System.out.println("source match count " + matchColumnCount);
			// System.out.println("source db total count " + totalColumnCount);
			// System.out.println("target db " + targetDB);
			// System.out.println("matching ratio " + ratio);
			// System.out.println("Matching IDs");
			// for (String id : ids) {
			// System.out.println(id);
			// }
			// System.out.println("");
			frame.addRow(new Object[] { sourceDB, targetDB, ratio });
		}
		this.dataframe = frame;
		this.dataframe.updateDataId();
		this.frameChanged = true;
		myStore.put("G", frame);

	}
	
	
	

	/**
	 * Displays the best fuzzy match between the instances values of a match for
	 * different algorithms
	 * 
	 * @param match
	 *            The match id in the format
	 *            (sourceProperty~)sourceConcept~sourceEngine%(targetProperty~)
	 *            targetConcept~targetEngine
	 */
	public void runFuzzyMatching(String match, int sampleSize) {

		// Parse input string
		// (sourceProperty~)sourceConcept~sourceEngine%(targetProperty~)targetConcept~targetEngine
		String[] parts = match.split("%");
		String[] source = parts[0].split("~");
		String[] target = parts[1].split("~");

		// Change order of the string to get engine-concept-property
		ArrayUtils.reverse(source);
		ArrayUtils.reverse(target);

		// Initialize
		String engineSource = "";
		String engineTarget = "";
		String conceptSource = "";
		String conceptTarget = "";
		String propertySource = "";
		String propertyTarget = "";

		boolean sourceIsProperty = false;
		boolean targetIsProperty = false;

		// Populate the values for source
		engineSource = source[0];
		conceptSource = source[1];
		if (source.length > 2) {
			propertySource = source[2];
			if (!propertySource.equals("none") && propertySource.length() > 0) {
				sourceIsProperty = true;
			}
		}

		// Populate the values for target
		engineTarget = target[0];
		conceptTarget = target[1];
		if (target.length > 2) {
			propertyTarget = target[2];
			if (!propertyTarget.equals("none") && propertyTarget.length() > 0) {
				targetIsProperty = true;
			}
		}

		// Initialize Engines
		IEngine iEngineSource = Utility.getEngine(engineSource.replaceAll(" ", "_"));
		IEngine iEngineTarget = Utility.getEngine(engineTarget.replaceAll(" ", "_"));

		// Get the source and target values
		
		// Source
		HashSet<String> sourceValues = new HashSet<String>();
		String conceptUriSource = DomainValues.getConceptURI(conceptSource, iEngineSource, false);
		if (sourceIsProperty) {
			String propertyUriSource = DomainValues.getPropertyURI(propertySource, conceptSource, iEngineSource, false);
			sourceValues = DomainValues.retrievePropertyUniqueValues(conceptUriSource, propertyUriSource,
					iEngineSource);
		} else {
			sourceValues = DomainValues.retrieveConceptUniqueValues(conceptUriSource, iEngineSource);
		}

		// Target
		HashSet<String> targetValues = new HashSet<String>();
		String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);
		if (targetIsProperty) {
			String propertyUriTarget = DomainValues.getPropertyURI(propertyTarget, conceptTarget, iEngineTarget, false);
			targetValues = DomainValues.retrievePropertyUniqueValues(conceptUriTarget, propertyUriTarget,
					iEngineTarget);
		} else {
			targetValues = DomainValues.retrieveConceptUniqueValues(conceptUriTarget, iEngineTarget);
		}
		// write two csvs source and target
		String csvSourcePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyMatching\\Temp\\fuzzySourceMatching.csv";
		csvSourcePath = csvSourcePath.replace("\\", "/");

		String csvTargetPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyMatching\\Temp\\fuzzyTargetMatching.csv";
		csvTargetPath = csvTargetPath.replace("\\", "/");
		// Construct headers based on existence of properties

		// Source
		String sourceHeader = "";
		if (sourceIsProperty) {
			sourceHeader = propertySource + "_" + conceptSource + "_" + engineSource.replace(" ", "_");
		} else {
			sourceHeader = conceptSource + "_" + engineSource.replace(" ", "_");
		}

		// Target
		String targetHeader = "";
		if (targetIsProperty) {
			targetHeader = propertyTarget + "_" + conceptTarget + "_" + engineTarget.replace(" ", "_");
		} else {
			targetHeader = conceptTarget + "_" + engineTarget.replace(" ", "_");
		}

		// Push to an array list for now
		// TODO refactor below to use set
		Object[] sourceArray = sourceValues.toArray();
		Object[] targetArray = targetValues.toArray();
		try {
			PrintWriter pw = new PrintWriter(new File(csvSourcePath));
			StringBuilder sb = new StringBuilder();
			sb.append(sourceHeader);

			sb.append("\n");
			for (int i = 0; i < sourceArray.length; i++) {
				String sourceInstance = "";
				if(sourceArray[i] != null) {
				sourceInstance = (String) sourceArray[i].toString();
				sourceInstance = sourceInstance.replaceAll("[^A-Za-z0-9 ]", "_");
				}
				sb.append(sourceInstance);
				sb.append("\n");
			}
			pw.write(sb.toString());
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		try {
			PrintWriter pw = new PrintWriter(new File(csvTargetPath));
			StringBuilder sb = new StringBuilder();
			sb.append(targetHeader);
			sb.append("\n");
			for (int i = 0; i < targetArray.length; i++) {
				String targetInstance = "";
				if(targetArray[i] != null) {
				targetInstance = (String) targetArray[i];
				targetInstance = targetInstance.replaceAll("[^A-Za-z0-9 ]", "_");
				}
				sb.append(targetInstance);
				sb.append("\n");
			}
			pw.write(sb.toString());
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// Run Fuzzy Matching in R
		String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyMatching\\best_match.r";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		runR("library(stringdist)");
		runR("source(\"" + utilityScriptPath + "\");");
		// create 2 frames from source and target csvs
		String df = "this.df.name.is.reserved.for.fuzzy.source.matching";
		runR(df + " <-read.csv(\"" + csvSourcePath + "\", na.strings=\"\")");
		String df2 = "this.df.name.is.reserved.for.fuzzy.target.matching";
		runR(df2 + " <-read.csv(\"" + csvTargetPath + "\", na.strings=\"\")");
		String outputDF = "this.df.name.is.reserved.for.fuzzy.output.matching";
		runR(outputDF + " <-match_metrics(" + df + " , " + df2 + " , \"" + sourceHeader + "\" , \"" + targetHeader + "\" , "
				+ sampleSize + ")");
		runR(outputDF.toString());
		// Retrieve
		storeVariable("GRID_NAME", outputDF);
		synchronizeFromR();
	}

	/**
	 * 
	 * @param match
	 *            The match id in the format
	 *            (sourceProperty~)sourceConcept~sourceEngine%(targetProperty~)
	 *            targetConcept~targetEngine
	 * @param join
	 *            join type inner, outer...
	 * @param method
	 *            matching method
	 * @param maxdist
	 * @param gramsize
	 * @param penalty
	 */
	public void runFuzzyJoin(String match, String join, String method, double maxdist, String gramsize, String penalty)
			throws FileNotFoundException {

		System.out.println(">>>Executing fuzzy join...");
		// Initialize
		String engineSource = "";
		String engineTarget = "";
		String conceptSource = "";
		String conceptTarget = "";
		String propertySource = "";
		String propertyTarget = "";

		// clean method for r script
		if (method.equals("Levenshtein")) {
			method = "lv";
		}
		if (method.equals("Damerau-Levenshtein")) {
			method = "dl";
		}
		if (method.equals("q-gram")) {
			method = "qgram";
		}
		if (method.equals("Cosine q-gram")) {
			method = "cosine";
		}
		if (method.equals("Optimal String Alignment")) {
			method = "osa";
		}
		if (method.equals("Jaro-Winker")) {
			method = "jw";
		}
		if (method.equals("Longest common substring")) {
			method = "lcs";
		}
		if (method.equals("Jaccard q-gram")) {
			method = "jaccard";
		}
		if (method.equals("Soundex")) {
			method = "soundex";
		}

		boolean sourceIsProperty = false;
		boolean targetIsProperty = false;

		// Parse input string
		String[] parts = match.split("%");
		String[] source = parts[0].split("~");
		String[] target = parts[1].split("~");

		// Change order of the string to get engine-concept-property
		// first create a list from String array

		List<String> list = Arrays.asList(source);
		List<String> list2 = Arrays.asList(target);

		// next, reverse the list using Collections.reverse method
		Collections.reverse(list2);
		Collections.reverse(list);

		source = (String[]) list.toArray();
		target = (String[]) list2.toArray();

		engineSource = source[0].replaceAll(" ", "_");
		conceptSource = source[1];
		if (source.length > 2) {
			propertySource = source[2];
			if (!propertySource.equals("none") && propertySource.length() > 0) {
				sourceIsProperty = true;
			}
		}

		engineTarget = target[0].replaceAll(" ", "_");
		conceptTarget = target[1];
		if (target.length > 2) {
			propertyTarget = target[2];
			if (!propertyTarget.equals("none") && propertyTarget.length() > 0) {
				targetIsProperty = true;
			}
		}

		// Initialize Engines
		IEngine iEngineSource = Utility.getEngine(engineSource);
		IEngine iEngineTarget = Utility.getEngine(engineTarget);

		// Get the source and target values

		// Source
		Vector<Object> sourceValues = new Vector<Object>();
		List<String> sourceProperties = new ArrayList<>();
		List<Object[]> allSourceInstances = null;
		Object[] sourceHeaders = null;
		String conceptUriSource = DomainValues.getConceptURI(conceptSource, iEngineSource, false);
		if (sourceIsProperty) {

			String propertyUriSource = DomainValues.getPropertyURI(propertySource, conceptSource, iEngineSource, false);
			sourceValues = (Vector<Object>) DomainValues.retrievePropertyValues(conceptUriSource, propertyUriSource,
					iEngineSource);
			allSourceInstances = new Vector<Object[]>();
			for (Object s : sourceValues) {
				Object[] row = new Object[1];
				row[0] = s;
				allSourceInstances.add(row);

			}
			sourceHeaders = new Object[1];
			sourceHeaders[0] = propertyUriSource;

		} else {
			sourceProperties = iEngineSource.getProperties4Concept(conceptUriSource, false);
			Vector<String> cleanPropertiesSource = new Vector<String>();
			for (String propVal : sourceProperties) {
				cleanPropertiesSource.add(DomainValues.determineCleanPropertyName(propVal, iEngineSource));

			}
			StringBuilder pkqlCommandSource = new StringBuilder();
			pkqlCommandSource.append("data.frame('grid');");
			pkqlCommandSource.append("data.import ");
			pkqlCommandSource.append("( api:");
			pkqlCommandSource.append(" " + engineSource + " ");
			pkqlCommandSource.append(". query ( [ c:");
			pkqlCommandSource.append(" " + conceptSource + " , ");
			for (int i = 0; i < cleanPropertiesSource.size(); i++) {
				pkqlCommandSource.append("c:" + " " + conceptSource + "__" + cleanPropertiesSource.get(i));
				if (i != cleanPropertiesSource.size() - 1) {
					pkqlCommandSource.append(" , ");
				}
			}
			pkqlCommandSource.append(" ] ) ) ;");
			pkqlCommandSource.append("panel[0].viz ( Grid, [ ");
			for (int i = 0; i < cleanPropertiesSource.size(); i++) {
				pkqlCommandSource.append("value= c:" + " " + cleanPropertiesSource.get(i));
				if (i < cleanPropertiesSource.size() - 1) {
					 pkqlCommandSource.append(" , ");
				}
			}
			pkqlCommandSource.append(" ] )  ;");
			System.out.println(pkqlCommandSource.toString());
			System.out.println("Running PKQL...");

			Insight insightSource = InsightUtility.createTemporaryInsight();
			InsightUtility.runPkql(insightSource, pkqlCommandSource.toString());
			ITableDataFrame data = (ITableDataFrame) insightSource.getDataMaker();
			allSourceInstances = data.getData();
			sourceHeaders = data.getColumnHeaders();
			System.out.println("RESULTS SUCCESS");

		}

		// Target
		Vector<Object> targetValues = new Vector<Object>();
		List<String> targetProperties = new ArrayList<>();
		List<Object[]> allTargetInstances = null;
		Object[] targetHeaders = null;
		String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);
		if (targetIsProperty) {

			String propertyUriTarget = DomainValues.getPropertyURI(propertyTarget, conceptTarget, iEngineTarget, false);
			targetValues = (Vector<Object>) DomainValues.retrievePropertyValues(conceptUriTarget, propertyUriTarget,
					iEngineTarget);
			allTargetInstances = new Vector<Object[]>();
			for (Object s : targetValues) {
				Object[] row = new Object[1];
				row[0] = s;
				allTargetInstances.add(row);

			}
			targetHeaders = new Object[1];
			targetHeaders[0] = propertyUriTarget;

		} else {
			targetProperties = iEngineTarget.getProperties4Concept(conceptUriTarget, false);
			Vector<String> cleanPropertiesTarget = new Vector<String>();
			for (String propVal : targetProperties) {
				cleanPropertiesTarget.add(DomainValues.determineCleanPropertyName(propVal, iEngineTarget));

			}
			StringBuilder pkqlCommandTarget = new StringBuilder();
			pkqlCommandTarget.append("data.frame('grid');");
			pkqlCommandTarget.append("data.import ");
			pkqlCommandTarget.append("( api:");
			pkqlCommandTarget.append(" " + engineTarget + " ");
			pkqlCommandTarget.append(". query ( [ c:");
			pkqlCommandTarget.append(" " + conceptTarget);
			if (cleanPropertiesTarget.size() != 0) {
				pkqlCommandTarget.append(" , ");
			}
			for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
				if (i != cleanPropertiesTarget.size() - 1) {
					pkqlCommandTarget.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i) + " , ");
				} else {
					pkqlCommandTarget.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i));
				}
			}
			pkqlCommandTarget.append(" ] ) ) ;");
			pkqlCommandTarget.append("panel[0].viz ( Grid, [ ");
			for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
				if (i != cleanPropertiesTarget.size() - 1) {
					pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i) + " , ");
				} else {
					pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i));
				}
			}
			pkqlCommandTarget.append(" ] )  ;");
			System.out.println(pkqlCommandTarget.toString());
			System.out.println("Running PKQL...");

			Insight insightTarget = InsightUtility.createTemporaryInsight();
			InsightUtility.runPkql(insightTarget, pkqlCommandTarget.toString());
			ITableDataFrame data = (ITableDataFrame) insightTarget.getDataMaker();
			allTargetInstances = data.getData();
			targetHeaders = data.getColumnHeaders();
			System.out.println("RESULTS SUCCESS");

		}

		String filePathSource = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoin\\Temp\\sourceDataFrame.txt";
		filePathSource = filePathSource.replace("\\", "/");

		String filePathTarget = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoin\\Temp\\targetDataFrame.txt";
		filePathTarget = filePathTarget.replace("\\", "/");

		// write source to file
		StringBuilder sv = new StringBuilder();
		for (int i = 0; i < sourceHeaders.length; i++) {
			sv.append(sourceHeaders[i].toString() + "_" + engineSource);
			if (i < sourceHeaders.length - 1) {
				sv.append("\t");

			}

		}
		sv.append("\n");

		for (int j = 0; j < allSourceInstances.size(); j++) {
			Object[] rowValue = allSourceInstances.get(j);
			for (int i = 0; i < rowValue.length; i++) {
				sv.append(rowValue[i].toString());
				if (i < rowValue.length - 1) {
					sv.append("\t");
				}
			}
			if (j < allSourceInstances.size() - 1) {
				sv.append("\n");
			}
		}

		PrintWriter printSource = new PrintWriter(new File(filePathSource));
		System.out.println(sv.toString());
		printSource.write(sv.toString());
		printSource.close();
		System.out.println(">>>SOURCE FILE PRINTED");

		// write target instances to file
		StringBuilder tv = new StringBuilder();
		for (int i = 0; i < targetHeaders.length; i++) {
			tv.append(targetHeaders[i].toString() + "_" + engineTarget);
			if (i < targetHeaders.length - 1) {
				tv.append("\t");

			}

		}
		tv.append("\n");

		for (int j = 0; j < allTargetInstances.size(); j++) {
			Object[] rowValue = allTargetInstances.get(j);
			for (int i = 0; i < rowValue.length; i++) {
				tv.append(rowValue[i].toString());
				if (i < rowValue.length - 1) {
					tv.append("\t");
				}
			}
			if (j < allTargetInstances.size() - 1) {
				tv.append("\n");
			}
		}

		PrintWriter printTarget = new PrintWriter(new File(filePathTarget));
		System.out.println(tv.toString());
		printTarget.write(tv.toString());
		printTarget.close();
		System.out.println(">>>TARGET FILE PRINTED");

		// Run Fuzzy Matching in R
		String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "FuzzyJoin\\fuzzy_join.r";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		runR("library(fuzzyjoin)");
		runR("source(\"" + utilityScriptPath + "\");");

		String df1 = "this.df.name.is.reserved.for.fuzzy.join.source";
		String df2 = "this.df.name.is.reserved.for.fuzzy.join.target";

		StringBuilder sourceRead = new StringBuilder();
		sourceRead.append(df1 + "<-read.table(\"" + filePathSource + "\"");
		sourceRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

		StringBuilder targetRead = new StringBuilder();
		targetRead.append(df2 + "<-read.table(\"" + filePathTarget + "\"");
		targetRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

		String df = "this.df.name.is.reserved.for.fuzzy.join.output";
		runR(sourceRead.toString());
		runR(targetRead.toString());

		String sourceHeader = "";
		if (sourceIsProperty) {
			sourceHeader = propertySource + "_" + conceptSource + "_" + engineSource.replace(" ", "_");
		} else {
			sourceHeader = conceptSource + "_" + engineSource.replace(" ", "_");
		}

		// Target
		String targetHeader = "";
		if (targetIsProperty) {
			targetHeader = propertyTarget + "_" + conceptTarget + "_" + engineTarget.replace(" ", "_");
		} else {
			targetHeader = conceptTarget + "_" + engineTarget.replace(" ", "_");
		}
		
		gramsize = "0";
		penalty = "0";

		// build the R command
		StringBuilder rCommand = new StringBuilder();
		rCommand.append(df);
		rCommand.append("<-fuzzy_join(");
		rCommand.append(df1 + ",");
		rCommand.append(df2 + ",");
		// TODO: expand this to select on any column, right now assumes the
		// first column
		rCommand.append("\"" + sourceHeader + "\"" + ",");
		rCommand.append("\"" + targetHeader + "\"" + ",");
		rCommand.append("\"" + join + "\"" + ",");
		rCommand.append(maxdist + ",");
		rCommand.append("method=" + "\"" + method + "\"" + ",");
		rCommand.append("q=" + gramsize + ",");
		rCommand.append("p=" + penalty + ")");
		System.out.println(rCommand.toString());
		runR(rCommand.toString());
		runR(df);
		storeVariable("GRID_NAME", df);
		synchronizeFromR();

	}

	
	/**
	 * This method is used 
	 * @param sourceInstances the list of instances to add to r dataframe to run fuzzy join
	 * @param target
	 * @param join
	 * @param method
	 * @param maxdist
	 * @param gramsize
	 * @param penalty
	 */
	public void runFuzzyJoinTest(String[] sourceInstances, String match, String join, String method, double maxdist,String gramsize,
			String penalty, boolean frameProperties) {
		// Initialize
		String engineSource = "";
		String engineTarget = "";
		String conceptSource = "";
		String conceptTarget = "";
		String propertySource = "";
		String propertyTarget = "";

		if (method.equals("Levenshtein")) {
			method = "lv";
		}
		if (method.equals("Damerau-Levenshtein")) {
			method = "dl";
		}
		if (method.equals("q-gram")) {
			method = "qgram";
		}
		if (method.equals("Cosine q-gram")) {
			method = "cosine";
		}
		if (method.equals("Optimal String Alignment")) {
			method = "osa";
		}
		if (method.equals("Jaro-Winker")) {
			method = "jw";
		}
		if (method.equals("Longest common substring")) {
			method = "lcs";
		}
		if (method.equals("Jaccard q-gram")) {
			method = "jaccard";
		}
		if (method.equals("Soundex")) {
			method = "soundex";
		}

		boolean sourceIsProperty = false;
		boolean targetIsProperty = false;

		// Parse input string
		String[] parts = match.split("%");
		String[] source = parts[0].split("~");
		String[] target = parts[1].split("~");

		// Change order of the string to get engine-concept-property
		// first create a list from String array

		List<String> list = Arrays.asList(source);
		List<String> list2 = Arrays.asList(target);

		// next, reverse the list using Collections.reverse method
		Collections.reverse(list2);
		Collections.reverse(list);

		source = (String[]) list.toArray();
		target = (String[]) list2.toArray();

		engineSource = source[0].replaceAll(" ", "_");
		conceptSource = source[1];
		if (source.length > 2) {
			propertySource = source[2];
			if (!propertySource.equals("none") && propertySource.length() > 0) {
				sourceIsProperty = true;
			}
		}

		engineTarget = target[0].replaceAll(" ", "_");
		conceptTarget = target[1];
		if (target.length > 2) {
			propertyTarget = target[2];
			if (!propertyTarget.equals("none") && propertyTarget.length() > 0) {
				targetIsProperty = true;
			}
		}

		// Initialize Engines
		IEngine iEngineSource = Utility.getEngine(engineSource);
		IEngine iEngineTarget = Utility.getEngine(engineTarget);

		// Get the source and target values

		// Source
		List<String> sourceProperties = new ArrayList<>();
		ArrayList<Object[]> allSourceInstances = new ArrayList<Object[]>();

		Object[] sourceHeaders = null;
		String conceptUriSource = DomainValues.getConceptURI(conceptSource, iEngineSource, false);
		if (sourceIsProperty) {
			for (int i = 0; i < sourceInstances.length; i++) {
				Object[] row = new Object[1];
				row[0] = sourceInstances[0];
				allSourceInstances.add(row);
			}
			sourceHeaders = new Object[1];
			sourceHeaders[0] = propertySource;

		} else {
			Vector<String> cleanPropertiesSource = new Vector<String>();

			// construct pkql to grab property values for selected instances
			if(frameProperties) {
			sourceProperties = iEngineSource.getProperties4Concept(conceptUriSource, false);
			for (String propVal : sourceProperties) {
				cleanPropertiesSource.add(DomainValues.determineCleanPropertyName(propVal, iEngineSource));

			}
			}
			sourceHeaders = new Object[sourceProperties.size() + 1];
			sourceHeaders[0] = conceptSource;
			if (frameProperties) {
				if (!sourceProperties.isEmpty()) {
					for (int i = 0; i < cleanPropertiesSource.size(); i++) {
						sourceHeaders[i + 1] = conceptSource + "__" + cleanPropertiesSource.get(i);
					}
				}
			}

			// check each instance if it has values, and if so add it to the
			// source instances
			StringBuilder pkqlSource = new StringBuilder();
			for (int i = 0; i < sourceInstances.length; i++) {
				if (frameProperties) {
				pkqlSource.append("data.frame ( 'grid' ) ;");
				pkqlSource.append("data.import ");
				pkqlSource.append("( api:");
				pkqlSource.append(" " + engineSource + " ");
				pkqlSource.append(". query ( [ c:");
				pkqlSource.append(" " + conceptSource);
			
					if (cleanPropertiesSource.size() != 0) {
						pkqlSource.append(" , ");
					}
					for (int j = 0; j < cleanPropertiesSource.size(); j++) {

						if (j != cleanPropertiesSource.size() - 1) {
							pkqlSource.append("c:" + " " + sourceHeaders[j + 1] + " , ");
						} else {
							pkqlSource.append("c:" + " " + sourceHeaders[j + 1]);
						}
					}
				
				pkqlSource.append(" ] , ( c: ");
				pkqlSource.append(conceptSource + " " + "=" + " " + "[ ");
				pkqlSource.append("\"" + sourceInstances[i] + "\" ");
				pkqlSource.append("] ) ) ) ;");
				System.out.println(pkqlSource.toString());
				System.out.println("Running PKQL...");
				Insight insightSource = InsightUtility.createTemporaryInsight();
				InsightUtility.runPkql(insightSource, pkqlSource.toString());
				ITableDataFrame data = (ITableDataFrame) insightSource.getDataMaker();
				if (!data.isEmpty()) {
					allSourceInstances.add(data.getData().get(0));
				} else {
					allSourceInstances.add(new Object[] { sourceInstances[i] });
				}
			}
			else {
				allSourceInstances.add(new Object[] { sourceInstances[i] });
			}

			}

			System.out.println("RESULTS SUCCESS");

		}

		// Target
		Vector<Object> targetValues = new Vector<Object>();
		List<String> targetProperties = new ArrayList<>();
		List<Object[]> allTargetInstances = null;
		Object[] targetHeaders = null;
		String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);

		if (targetIsProperty) {
			String propertyUriTarget = DomainValues.getPropertyURI(propertyTarget, conceptTarget, iEngineTarget, false);
			targetValues = (Vector<Object>) DomainValues.retrieveCleanPropertyValues(conceptUriTarget,
					propertyUriTarget, iEngineTarget);
			for (Object s : targetValues) {
				Object[] row = new Object[1];
				row[0] = s;
				allTargetInstances.add(row);

			}
			targetHeaders = new Object[1];
			targetHeaders[0] = propertyTarget;

		} else {
			Vector<String> cleanPropertiesTarget = new Vector<String>();
			if(frameProperties) {
			targetProperties = iEngineTarget.getProperties4Concept(conceptUriTarget, false);
			
			for (String propVal : targetProperties) {
				cleanPropertiesTarget.add(DomainValues.determineCleanPropertyName(propVal, iEngineTarget));

			}
			}

			StringBuilder pkqlCommandTarget = new StringBuilder();
			pkqlCommandTarget.append("data.frame('grid');");
			pkqlCommandTarget.append("data.import ");
			pkqlCommandTarget.append("( api:");
			pkqlCommandTarget.append(" " + engineTarget + " ");
			pkqlCommandTarget.append(". query ( [ c:");
			pkqlCommandTarget.append(" " + conceptTarget);
			if (frameProperties) {
				if (cleanPropertiesTarget.size() != 0) {
					pkqlCommandTarget.append(" , ");
				}
				for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
					if (i != cleanPropertiesTarget.size() - 1) {
						pkqlCommandTarget
								.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i) + " , ");
					} else {
						pkqlCommandTarget.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i));
					}
				}
			}
			pkqlCommandTarget.append(" ] ) ) ;");
			pkqlCommandTarget.append("panel[0].viz ( Grid, [ ");
			pkqlCommandTarget.append("value= c: " + conceptTarget + ", ");
			for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
				if (i != cleanPropertiesTarget.size() - 1) {
					pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i) + " , ");
				} else {
					pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i));
				}
			}
			pkqlCommandTarget.append(" ] )  ;");
			System.out.println(pkqlCommandTarget.toString());
			System.out.println("Running PKQL...");

			Insight insightTarget = InsightUtility.createTemporaryInsight();
			InsightUtility.runPkql(insightTarget, pkqlCommandTarget.toString());
			ITableDataFrame data = (ITableDataFrame) insightTarget.getDataMaker();
			allTargetInstances = data.getData();
			targetHeaders = data.getColumnHeaders();
			System.out.println("RESULTS SUCCESS");

		}

		String filePathSource = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoinTest\\Temp\\sourceDataFrame.txt";
		filePathSource = filePathSource.replace("\\", "/");

		String filePathTarget = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoinTest\\Temp\\targetDataFrame.txt";
		filePathTarget = filePathTarget.replace("\\", "/");

		// write source to file
		StringBuilder sv = new StringBuilder();
		for (int i = 0; i < sourceHeaders.length; i++) {
			sv.append(sourceHeaders[i].toString() + "_" + engineSource);
			if (i < sourceHeaders.length - 1) {
				sv.append("\t");

			}

		}
		sv.append("\n");

		for (int j = 0; j < allSourceInstances.size(); j++) {
			Object[] rowValue = allSourceInstances.get(j);
			for (int i = 0; i < rowValue.length; i++) {
				Object value = rowValue[i];
				if (value != null) {
					sv.append(rowValue[i].toString());
				} else {
					sv.append("");
				}
				if (i < rowValue.length - 1) {
					sv.append("\t");
				}
			}
			if (j < allSourceInstances.size() - 1) {
				sv.append("\n");
			}
		}

		PrintWriter printSource;
		try {
			printSource = new PrintWriter(new File(filePathSource));
			printSource.write(sv.toString());
			printSource.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// write target instances to file
		StringBuilder tv = new StringBuilder();
		for (int i = 0; i < targetHeaders.length; i++) {
			tv.append(targetHeaders[i].toString() + "_" + engineTarget);
			if (i < targetHeaders.length - 1) {
				tv.append("\t");

			}

		}
		tv.append("\n");

		for (int j = 0; j < allTargetInstances.size(); j++) {
			Object[] rowValue = allTargetInstances.get(j);
			for (int i = 0; i < rowValue.length; i++) {
				Object value = rowValue[i];
				if(value != null) {
				tv.append(rowValue[i].toString());
				}
				else {
					tv.append("");

				}
				if (i < rowValue.length - 1) {
					tv.append("\t");
				}
			}
			if (j < allTargetInstances.size() - 1) {
				tv.append("\n");
			}
		}

		PrintWriter printTarget;
		try {
			printTarget = new PrintWriter(new File(filePathTarget));
			printTarget.write(tv.toString());
			printTarget.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String sourceHeader = "";
		if (sourceIsProperty) {
			sourceHeader = propertySource + "_" + conceptSource + "_" + engineSource.replace(" ", "_");
		} else {
			sourceHeader = conceptSource + "_" + engineSource.replace(" ", "_");
		}

		// Target
		String targetHeader = "";
		if (targetIsProperty) {
			targetHeader = propertyTarget + "_" + conceptTarget + "_" + engineTarget.replace(" ", "_");
		} else {
			targetHeader = conceptTarget + "_" + engineTarget.replace(" ", "_");
		}

		// Run Fuzzy Matching in R
		String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoinTest\\fuzzy_single_join.r";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");
		method = "jw";
		runR("library(fuzzyjoin)");
		runR("source(\"" + utilityScriptPath + "\");");

		String df1 = "this.df.name.is.reserved.for.fuzzy.join.source";
		String df2 = "this.df.name.is.reserved.for.fuzzy.join.target";

		StringBuilder sourceRead = new StringBuilder();
		sourceRead.append(df1 + "<-read.table(\"" + filePathSource + "\"");
		sourceRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

		StringBuilder targetRead = new StringBuilder();
		targetRead.append(df2 + "<-read.table(\"" + filePathTarget + "\"");
		targetRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

		String df = "this.df.name.is.reserved.for.fuzzy.join.output";
		runR(sourceRead.toString());
		runR(targetRead.toString());

		method = "jw";
		maxdist = 1 - maxdist;
		String maxDistStr = "" + maxdist;
		// join = "left";
		gramsize = "0";
		penalty = "0";

		// build the R command
		StringBuilder rCommand = new StringBuilder();
		rCommand.append(df);
		rCommand.append("<-fuzzy_join(");
		rCommand.append(df1 + ",");
		rCommand.append(df2 + ",");
		rCommand.append("\"" + sourceHeader + "\"" + ",");
		rCommand.append("\"" + targetHeader + "\"" + ",");
		rCommand.append("\"" + join + "\"" + ",");
		rCommand.append(maxDistStr + ",");
		rCommand.append("method=" + "\"" + method + "\"" + ",");
		rCommand.append("q=" + gramsize + ",");
		rCommand.append("p=" + penalty + ")");
		// System.out.println(rCommand.toString());
		runR(rCommand.toString());
		runR(df);
		storeVariable("GRID_NAME", df);
		synchronizeFromR();
	}
	

	
    public void runBusinessRules(String match, String supportValue, String confidenceValue) throws FileNotFoundException {
        String engineSource = "";
        String engineTarget = "";
        String conceptSource = "";
        String conceptTarget = "";
        //check valid values for now
        float support = Float.parseFloat(supportValue);
        float confidence = Float.parseFloat(confidenceValue);
        
        
        //default values
        if(support > 1 || support < 0) {
               support = (float) 0.05;
        }
        
        if (confidence > 1 || confidence < 0) {
               confidence = (float) 0.8;
        }
        

        // Parse input string
        String[] parts = match.split("%");
        String[] source = parts[0].split("~");
        String[] target = parts[1].split("~");

        // Change order of the string to get engine-concept-property
        // first create a list from String array

        List<String> list = Arrays.asList(source);
        List<String> list2 = Arrays.asList(target);

        // next, reverse the list using Collections.reverse method
        Collections.reverse(list2);
        Collections.reverse(list);

        source = (String[]) list.toArray();
        target = (String[]) list2.toArray();

        engineSource = source[0].replace(" ", "_");
        conceptSource = source[1];

        engineTarget = target[0].replace(" ", "_");
        conceptTarget = target[1];

        // initialize engines
        IEngine iEngineSource = Utility.getEngine(engineSource.replaceAll(" ", "_"));
        IEngine iEngineTarget = Utility.getEngine(engineTarget.replaceAll(" ", "_"));

        // Source
        List<String> sourceProperties = new ArrayList<>();
        List<Object[]> allSourceInstances = null;
        Object[] sourceHeaders = null;
        String conceptUriSource = DomainValues.getConceptURI(conceptSource, iEngineSource, false);

        sourceProperties = iEngineSource.getProperties4Concept(conceptUriSource, false);
        Vector<String> cleanPropertiesSource = new Vector<String>();
        for (String propVal : sourceProperties) {
               cleanPropertiesSource.add(DomainValues.determineCleanPropertyName(propVal, iEngineSource));

        }

        StringBuilder pkqlCommandSource = new StringBuilder();
        pkqlCommandSource.append("data.frame('grid');");
        pkqlCommandSource.append("data.import ");
        pkqlCommandSource.append("( api:");
        pkqlCommandSource.append(" " + engineSource + " ");
        pkqlCommandSource.append(". query ( [ c:");
        pkqlCommandSource.append(" " + conceptSource + " ");
        
        for (int i = 0; i < cleanPropertiesSource.size(); i++) {
        	pkqlCommandSource.append(", ");
            pkqlCommandSource.append("c:" + " " + conceptSource + "__" + cleanPropertiesSource.get(i));
               
        }
        pkqlCommandSource.append(" ] ) ) ;");
        
        /**
        pkqlCommandSource.append("panel[0].viz ( Grid, [ ");
        for (int i = 0; i < cleanPropertiesSource.size(); i++) {
               if (i != cleanPropertiesSource.size() - 1) {
                     pkqlCommandSource.append("value= c:" + " " + cleanPropertiesSource.get(i) + " , ");
               } else {
                     pkqlCommandSource.append("value= c:" + " " + cleanPropertiesSource.get(i));
               }
        }
        pkqlCommandSource.append(" ] )  ;");
        **/
        System.out.println(pkqlCommandSource.toString());
        System.out.println("Running PKQL...");

        Insight insightSource = InsightUtility.createTemporaryInsight();
        InsightUtility.runPkql(insightSource, pkqlCommandSource.toString());
        ITableDataFrame data = (ITableDataFrame) insightSource.getDataMaker();
        allSourceInstances = data.getData();
        sourceHeaders = data.getColumnHeaders();
        System.out.println("RESULTS SUCCESS");

        // target
        List<String> targetProperties = new ArrayList<>();
        List<Object[]> allTargetInstances = null;
        Object[] targetHeaders = null;
        String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);

        targetProperties = iEngineTarget.getProperties4Concept(conceptUriTarget, false);
        Vector<String> cleanPropertiesTarget = new Vector<String>();
        for (String propVal : targetProperties) {
               cleanPropertiesTarget.add(DomainValues.determineCleanPropertyName(propVal, iEngineTarget));

        }
        StringBuilder pkqlCommandTarget = new StringBuilder();
        pkqlCommandTarget.append("data.frame('grid');");
        pkqlCommandTarget.append("data.import ");
        pkqlCommandTarget.append("( api:");
        pkqlCommandTarget.append(" " + engineTarget + " ");
        pkqlCommandTarget.append(". query ( [ c:");
        pkqlCommandTarget.append(" " + conceptTarget);
        if (cleanPropertiesTarget.size() != 0) {
               pkqlCommandTarget.append(" , ");
        }
        for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
               if (i != cleanPropertiesTarget.size() - 1) {
                     pkqlCommandTarget.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i) + " , ");
               } else {
                     pkqlCommandTarget.append("c:" + " " + conceptTarget + "__" + cleanPropertiesTarget.get(i));
               }
        }
        pkqlCommandTarget.append(" ] ) ) ;");
        pkqlCommandTarget.append("panel[0].viz ( Grid, [ ");
        for (int i = 0; i < cleanPropertiesTarget.size(); i++) {
               if (i != cleanPropertiesTarget.size() - 1) {
                     pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i) + " , ");
               } else {
                     pkqlCommandTarget.append("value= c:" + " " + cleanPropertiesTarget.get(i));
               }
        }
        pkqlCommandTarget.append(" ] )  ;");
        System.out.println(pkqlCommandTarget.toString());
        System.out.println("Running PKQL...");

        Insight insightTarget = InsightUtility.createTemporaryInsight();
        InsightUtility.runPkql(insightTarget, pkqlCommandTarget.toString());
        ITableDataFrame dataTarget = (ITableDataFrame) insightTarget.getDataMaker();
        allTargetInstances = dataTarget.getData();
        targetHeaders = dataTarget.getColumnHeaders();
        System.out.println("RESULTS SUCCESS");
        
        String filePathSource = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
                     + "BusinessRules\\Temp\\sourceDataFrame_businessRules.txt";
        filePathSource = filePathSource.replace("\\", "/");

        String filePathTarget = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
                     + "BusinessRules\\Temp\\targetDataFrame_businessRules.txt";
        filePathTarget = filePathTarget.replace("\\", "/");

        // write source to file
        StringBuilder sv = new StringBuilder();
        for (int i = 0; i < sourceHeaders.length; i++) {
               sv.append(sourceHeaders[i].toString() + "_" + engineSource);
               if (i < sourceHeaders.length - 1) {
                     sv.append("\t");

               }

        }
        sv.append("\n");

        for (int j = 0; j < allSourceInstances.size(); j++) {
               Object[] rowValue = allSourceInstances.get(j);
               for (int i = 0; i < rowValue.length; i++) {
                     sv.append(rowValue[i].toString());
                     if (i < rowValue.length - 1) {
                            sv.append("\t");
                     }
               }
               if (j < allSourceInstances.size() - 1) {
                     sv.append("\n");
               }
        }

        PrintWriter printSource = new PrintWriter(new File(filePathSource));
        System.out.println(sv.toString());
        printSource.write(sv.toString());
        printSource.close();
        System.out.println(">>>SOURCE FILE PRINTED");

        // write target instances to file
        StringBuilder tv = new StringBuilder();
        for (int i = 0; i < targetHeaders.length; i++) {
               tv.append(targetHeaders[i].toString() + "_" + engineTarget);
               if (i < targetHeaders.length - 1) {
                     tv.append("\t");

               }

        }
        tv.append("\n");

        for (int j = 0; j < allTargetInstances.size(); j++) {
               Object[] rowValue = allTargetInstances.get(j);
               for (int i = 0; i < rowValue.length; i++) {
                     tv.append(rowValue[i].toString());
                     if (i < rowValue.length - 1) {
                            tv.append("\t");
                     }
               }
               if (j < allTargetInstances.size() - 1) {
                     tv.append("\n");
               }
        }

        PrintWriter printTarget = new PrintWriter(new File(filePathTarget));
        System.out.println(tv.toString());
        printTarget.write(tv.toString());
        printTarget.close();
        System.out.println(">>>TARGET FILE PRINTED");
        
        
        
        /** START R PROCESSING **/
        
        String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "BusinessRules\\etl _rules.r";
        utilityScriptPath = utilityScriptPath.replace("\\", "/");

        runR("library(arules)");
        runR("source(\"" + utilityScriptPath + "\");");

        String df1 = "this.df.name.is.reserved.for.compatibility.source";
        String df2 = "this.df.name.is.reserved.for.compatibility.target";
        
        StringBuilder sourceRead = new StringBuilder();
        sourceRead.append(df1 + "<-read.table(\"" + filePathSource + "\"");
        sourceRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
                     + "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

         StringBuilder targetRead = new StringBuilder();
        targetRead.append(df2 + "<-read.table(\"" + filePathTarget + "\"");
        targetRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
                     + "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");

        String df = "this.df.name.is.reserved.for.compatibility.output";
        String listSourceName = "this.name.is.reserved.for.source.list";
        String listTargetName = "this.name.is.reserved.for.target.list";
        String listSource = listSourceName + "<-c(" + "\"" + conceptSource + "_" + engineSource + "\"" + ")";
        String listTarget = listTargetName + "<-c(" + "\"" + conceptTarget + "_" + engineTarget + "\"" + ")";
        runR(sourceRead.toString());
        runR(targetRead.toString());
        runR(listSource);
        runR(listTarget);
        
  
//      String sourceHeader = conceptSource + "_" + engineSource.replace(" ", "_");
//      String targetHeader = conceptTarget + "_" + engineTarget.replace(" ", "_");
        StringBuilder rCommand = new StringBuilder();
        rCommand.append(df+ "<-etl_rules(");
        rCommand.append(df1);
        rCommand.append("," + df2);
        rCommand.append("," + listSourceName + "," + listTargetName + ",");
        rCommand.append(support + "," + confidence);
        rCommand.append(")");
        System.out.println(rCommand.toString());
        runR(rCommand.toString());
        storeVariable("GRID_NAME", df);
        synchronizeFromR();

  }
    
	public void runXrayNetwork(String[] engines, double pkiThreshold, double similarityScore) {

		// Persist the data into a database
		String matchingDbName = "MatchingRDBMSDatabase";
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(matchingDbName);

		H2Frame frameToUse = new H2Frame();
		String[] colNames = new String[] { "source_table_id", "source_database", "target_table_id", "target_database", "match_id" };
		String[] colTypes = new String[] { "STRING", "STRING", "STRING", "STRING", "STRING" };

		QueryStruct qs = new QueryStruct();
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		for (int i = 0; i < colNames.length; i++) {
			dataTypeMapStr.put(colNames[i], colTypes[i]);
			dataTypeMap.put(colNames[i], Utility.convertStringToDataType(colTypes[i]));
			qs.addSelector(colNames[i], null);
		}

		// frameToUse.setUserId(schemaName);
		((H2Frame) dataframe).dropTable();

		// this is set when we set the original dataframe
		// within the reactor
		frameToUse.setUserId(this.userId);

		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
		frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);
		
		if (engine != null) {
			String query = "SELECT source_table_id, source_database, target_table_id, target_database, match_id";
			query += " FROM match_id WHERE ";
			
			//add engines to query
			query += "source_database IN( ";
			for (int i = 0; i < engines.length; i++) {
				query += "'" + engines[i] + "'";
				if(i < engines.length - 1) {
					query += ",";
				}
			}
			query += ") ";
			
			//add pki threshold to query
			query += "AND PKI >= " + pkiThreshold + " ";
			
			//add similarity to query
			query += "AND SCORE >= " +similarityScore + ";";
			
			Map<String, Object> values = engine.execQuery(query);
			ResultSet rs = (ResultSet) values.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			ResultSetMetaData rsmd;
			try {
				rsmd = rs.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				Object[] data = new Object[columnsNumber];

				while (rs.next()) {
					// double pki = rs.getDouble(1);
					String sourceTable = rs.getString(1);
					// clean itemConcept
					String sourceDB = rs.getString(2);
					String targetTable = rs.getString(3);
					if (targetTable.contains("%%%")) {
						String[] temp = targetTable.split("%%%");
						String[] engineSplit = temp[1].split("~");
						targetTable = temp[0] + "~" + engineSplit[1];
					}
					String targetDB = rs.getString(4);
					String matchID = rs.getString(5);
					frameToUse.addRow(new Object[] { sourceTable, sourceDB, targetTable, targetDB, matchID });

				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// frameToUse.addRows(dataIterator, dataTypeMap);
			// dataIterator.deleteFile();

			this.dataframe = frameToUse;
			this.put("G", frameToUse);
			this.frameChanged = true;
			this.dataframe.updateDataId();

		}

	}
	
	public void updateOWL(String sourceDB, String sourceColumn, String targetDB, String targetColumn) {
		IEngine sourceEngine = Utility.getEngine(sourceDB);
		IEngine targetEngine = Utility.getEngine(targetDB);
		if (sourceEngine != null && targetEngine != null) {
			OWLER sourceOWL = new OWLER(sourceEngine, sourceEngine.getOWL());
			String sourceConceptualURI = DomainValues.getConceptURI(sourceColumn, sourceEngine, true);
			String sourcePhysicalURI = sourceEngine.getPhysicalUriFromConceptualUri(sourceConceptualURI);

			String targetConceptualURI = DomainValues.getConceptURI(targetColumn, targetEngine, true);
			String tagetPhysicalURI = targetEngine.getPhysicalUriFromConceptualUri(targetConceptualURI);

			if (sourceOWL != null) {
				String owlStr = sourceOWL.getOwlAsString();
				String sourceConceptualREGEX = "\\\"";
				String[] sourceURIparts = sourceConceptualURI.split("/");
				for (int i = 0; i < sourceURIparts.length; i++) {
					if (sourceURIparts[i].length() > 0) {
						sourceConceptualREGEX += sourceURIparts[i];
						if (i < sourceURIparts.length - 1) {
							sourceConceptualREGEX += "\\/";
						}
					} else {
						sourceConceptualREGEX += "\\/";
					}

				}
				sourceConceptualREGEX += "\\\"";
				String pattern = sourceConceptualREGEX;

				// Create a Pattern object
				Pattern r = Pattern.compile(pattern);

				// Now create matcher object.
				Matcher m = r.matcher(owlStr);
				if (m.find()) {
					String newOwlStr = m.replaceAll("\"" + targetConceptualURI + "\"");
					String sourceOwlPath = sourceOWL.getOwlPath();
					try {
						PrintWriter writer = new PrintWriter(sourceOWL.getOwlPath(), "UTF-8");
						writer.print(newOwlStr);
						writer.close();
//						try {
//							sourceOWL.commit();
//							sourceOWL.export();
							sourceEngine.setOWL(sourceOWL.getOwlPath());
							sourceOWL.export();
							
						
							
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
						//sourceOWL.closeOwl();
					} catch (IOException e) {
						// do something
					}

					System.out.println("Replaced OWL");

				} else {
					System.out.println("Unable tosafd update OWL");
				}
			}
		}
	}
	
	
	public String runXrayCompatibility(String selectedInfoJson, double similarityThreshold, double candidateThreshold, boolean matchingSameDB)
			throws SQLException, JsonParseException, JsonMappingException, IOException {

		// runs the full xray compatibility from the new UI
		HashMap<String, Object> selectedInfo = new ObjectMapper().readValue(selectedInfoJson, HashMap.class);

		// TODO regenerate?
		String metadataFile = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\XrayCompatibility" + "\\"
				+ Constants.R_TEMP_FOLDER + "\\instanceCount.csv";
		metadataFile = metadataFile.replace("\\", "/");

		String outputFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "XrayCompatibility\\Temp\\MatchingRepository";
		// clean output folder
		try {
			FileUtils.cleanDirectory(new File(outputFolder));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// write text files for all connections
		for (Object url : selectedInfo.keySet()) {
			String connectionUrl = (String) url;
			HashMap<String, Object> urlInfo = (HashMap<String, Object>) selectedInfo.get(url);
			// bifurcation in processing occurs if input is an engine or jdbc

			String schemaType = (String) urlInfo.get("databaseType"); 
			if (schemaType.equalsIgnoreCase("external")) {
				// process if jdbc connection
				Connection con = null;
				String driverType = Utility.getRDBMSDriverType(connectionUrl);

				try {
					// instantiate the correct rdbms driver depending on type
					if (driverType.equals("mysql")) {
						Class.forName("com.mysql.jdbc.Driver");
					} else if (driverType.equals("oracle")) {
						Class.forName("oracle.jdbc.driver.OracleDriver");
					} else if (driverType.equals("sqlserver")) {
						Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
					} else if (driverType.equals("db2")) {
						Class.forName("com.ibm.db2.jcc.DB2Driver");
					} else if (driverType.equals("h2")) {
						Class.forName("org.h2.Driver");
					} else {
						System.out.println(">>>Invalid connection string");
					}

					// build connection

					con = DriverManager.getConnection(connectionUrl, "sa", "");

				} catch (ClassNotFoundException e) {
					System.out.println(">>>JDBC Driver not found, please ensure you have drivers installed.");
					e.printStackTrace();
				}
				ArrayList<String> colInfo = (ArrayList<String>) selectedInfo.get(connectionUrl);
				for (String col : colInfo) {
					String[] parts = col.split("__");
					String db = parts[0];
					String tableName = parts[1];
					String columnName = parts[2];

					// build sql query - write only unique values
					
					StringBuilder sb = new StringBuilder();
					sb.append("SELECT DISTINCT ");
					sb.append(columnName);
					sb.append(" FROM ");
					sb.append(tableName);
					sb.append(";");
					String query = sb.toString();

					// execute query against db
					Statement stmt = null;

					try {
						stmt = con.createStatement();
						ResultSet rs = stmt.executeQuery(query);
						String fileName = parts[0] + ";" + parts[1] + ";" + parts[2];
						String testFilePath = outputFolder + fileName + ".txt";
						testFilePath = testFilePath.replace("\\", "/");
						String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
								+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
						minHashFilePath = minHashFilePath.replace("\\", "/");
						runR("library(textreuse)");
						runR("source(" + "\"" + minHashFilePath + "\"" + ");");
						List<Object> instances = Utility.getInstancesFromRs(rs);
						StringBuilder rsb = new StringBuilder();

						// construct R dataframe
						String dfName = "df.xray";
						runR(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
						for (int i = 0; i < instances.size(); i++) {
							// ugh why doesn't R have zero-based indexing
							rsb.append(dfName + "[" + (i + 1) + ",1" + "]");
							rsb.append("<-");
							rsb.append("\"" + instances.get(i) + "\"");
							rsb.append(";");

						}
						runR(rsb.toString());
						runR("encode_instances(" + dfName + "," + "\"" + testFilePath + "\"" + ");");

						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				con.close();

			} else {
				// process if an engine
				// grab concepts and properties
				List<String> selectedColumns = (List<String>) urlInfo.get("columns");
				List<String> conceptList = new ArrayList<String>();
				List<String> propertyList = new ArrayList<String>();

				for (String col : selectedColumns) {
					String tableName = col.split("__")[0];
					String colName = col.split("__")[1];

					if (tableName.equals(colName)) {
						// concept case
						conceptList.add(tableName);
					} else {
						propertyList.add(col);
					}
				}

				DomainValues dv = new DomainValues();
				IEngine engine = Utility.getEngine(connectionUrl);
				// get instance values and write to csv using minHash in R
				for (String concept : conceptList) {
					String fileName = connectionUrl + ";" + concept + ";";
					String testFilePath = outputFolder + "\\" + fileName + ".txt";
					testFilePath = testFilePath.replace("\\", "/");
					String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
							+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
					minHashFilePath = minHashFilePath.replace("\\", "/");
					String uri = DomainValues.getConceptURI(concept, engine, true);
					List<Object> instances;
					if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
						instances = DomainValues.retrieveCleanConceptValues(uri, engine);
					} else {
						instances = DomainValues.retrieveCleanConceptValues(concept, engine);
					}
					StringBuilder rsb = new StringBuilder();
					rsb.append("library(textreuse);");
					rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");

					// construct R dataframe
					String dfName = "df.xray";
					rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");

					for (int i = 0; i < instances.size(); i++) {
						rsb.append(dfName + "[" + (i + 1) + ",1" + "]");
						rsb.append("<-");
						rsb.append("\"" + instances.get(i) + "\"");
						rsb.append(";");

					}
					rsb.append("encode_instances(" + dfName + "," + "\"" + testFilePath + "\"" + ");");
					runR(rsb.toString());

				}

				for (String prop : propertyList) {
					String concept = prop.split("__")[0];
					String property = prop.split("__")[1];
					String fileName = connectionUrl + ";" + concept + ";" + property;
					String testFilePath = outputFolder + "\\" + fileName + ".txt";
					testFilePath = testFilePath.replace("\\", "/");
					String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
							+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
					minHashFilePath = minHashFilePath.replace("\\", "/");
					// TODO rdf?
					String conceptUri = DomainValues.getConceptURI(concept, engine, true);

					String propUri = DomainValues.getPropertyURI(property, concept, engine, false);

					List<Object> instances;
					if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
						instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
					} else {
						instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
					}
					StringBuilder rsb = new StringBuilder();
					rsb.append("library(textreuse);");
					rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");

					// construct R dataframe
					String dfName = "df.xray";
					rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");

					for (int i = 0; i < instances.size(); i++) {
						rsb.append(dfName + "[" + (i + 1) + ",1" + "]");
						rsb.append("<-");
						rsb.append("\"" + instances.get(i) + "\"");
						rsb.append(";");

					}
					rsb.append("encode_instances(" + dfName + "," + "\"" + testFilePath + "\"" + ");");
					runR(rsb.toString());
				}

			}
		}
		String baseMatchingFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "XrayCompatibility";
		baseMatchingFolder = baseMatchingFolder.replace("\\", "/");

		// Grab the corpus directory
		String corpusDirectory = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "XrayCompatibility\\Temp\\MatchingRepository";
		corpusDirectory = corpusDirectory.replace("\\", "/");

		// Grab the csv directory Semoss/R/Matching/Temp/rdf
		String baseRDFDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdf";
		baseRDFDirectory = baseRDFDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdf/MatchingCsvs
		String rdfCsvDirectory = baseRDFDirectory + "\\" + Constants.R_MATCHING_CSVS_FOLDER;
		rdfCsvDirectory = rdfCsvDirectory.replace("\\", "/");

		// Grab the prop directory Semoss/R/Matching/Temp/rdf/MatchingProp
		String rdfPropDirectory = baseRDFDirectory + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_MATCHING_PROP_FOLDER;
		rdfPropDirectory = rdfPropDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdbms
		String rdbmsDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdbms";
		rdbmsDirectory = rdbmsDirectory.replace("\\", "/");

		int nMinhash;
		int nBands;
		int instancesThreshold = 1;

		if (similarityThreshold < 0 || similarityThreshold > 1) {
			similarityThreshold = 0.7;
		}

		if (candidateThreshold < 0 || candidateThreshold > 1) {
			candidateThreshold = 0.15;
		}

		// set other parameters
		if (candidateThreshold <= 0.03) {
			nMinhash = 3640;
			nBands = 1820;
		} else if (candidateThreshold <= 0.05) {
			nMinhash = 1340;
			nBands = 670;
		} else if (candidateThreshold <= 0.1) {
			nMinhash = 400;
			nBands = 200;
		} else if (candidateThreshold <= 0.2) {
			nMinhash = 200;
			nBands = 100;
		} else if (candidateThreshold <= 0.4) {
			nMinhash = 210;
			nBands = 70;
		} else if (candidateThreshold <= 0.5) {
			nMinhash = 200;
			nBands = 50;
		} else {
			nMinhash = 200;
			nBands = 40;
		}

		// Parameters for R script
		String rFrameName = "this.dt.name.is.reserved.for.semantic.matching";

		// Grab the utility script
		String utilityScriptPath = baseMatchingFolder + "\\" + "matching.R";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		runR("library(textreuse)");

		// Source the LSH function from the utility script
		runR("source(\"" + utilityScriptPath + "\");");
		runR(rFrameName + " <- data.frame()");

		// check if user wants to compare columns from the same database
		String matchingSameDBR = "FALSE";
		if (matchingSameDB) {
			matchingSameDBR = "TRUE";
		}

		// Run locality sensitive hashing to generate matches
		runR(rFrameName + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + corpusDirectory + "\", " + nMinhash + ", "
				+ nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
				+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchingSameDBR + ", \"" + rdbmsDirectory
				+ "\", \"" + metadataFile + "\")");

		// Synchronize from R
		storeVariable("GRID_NAME", rFrameName);
		synchronizeFromR();

		// List<Object[]> returnArray = new ArrayList<Object[]>();
		// returnArray.add(dataframe.getColumnHeaders());
		//
		// for (Object[] obj : dataframe.getData()) {
		// returnArray.add(obj);
		// }
		//
		// ObjectWriter ow = new
		// ObjectMapper().writer().withDefaultPrettyPrinter();
		// this.returnData = ow.writeValueAsString(returnArray);

		// TODO save to local master?

		// // Persist the data into a database
		// String matchingDbName = "MatchingRDBMSDatabase";
		// IEngine engine = Utility.getEngine(matchingDbName);
		//
		// // Only add to the engine if it is null
		// // TODO gracefully refresh the entire db
		// if (engine == null) {
		// MatchingDB db = new MatchingDB(getBaseFolder());
		// // creates rdf and rdbms dbs
		// // TODO specify dbType if desired
		// String matchingDBType = ImportOptions.DB_TYPE.RDBMS.toString();
		// db.saveDB(matchingDBType);
		//
		// }
		// this.hasReturnData = true;
		return "";
		// return null;
	}

	
	public String runXrayCompatibility(String configFileJson)
			throws SQLException, JsonParseException, JsonMappingException, IOException {

		// runs the full xray compatibility from the new UI
		HashMap<String, Object> config = new ObjectMapper().readValue(configFileJson, HashMap.class);
		HashMap<String, Object> parameters = (HashMap<String, Object>) config.get("parameters");
		// TODO regenerate?
		String metadataFile = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\XrayCompatibility" + "\\"
				+ Constants.R_TEMP_FOLDER + "\\instanceCount.csv";
		metadataFile = metadataFile.replace("\\", "/");

		String outputFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "XrayCompatibility\\Temp\\MatchingRepository";
		// clean output folder
		try {
			FileUtils.cleanDirectory(new File(outputFolder));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		ArrayList<Object> connectors = (ArrayList<Object>) config.get("connectors");
		for (int i = 0; i < connectors.size(); i++) {
			HashMap<String, Object> connection = (HashMap<String, Object>) connectors.get(i);
			String connectorType = (String) connection.get("connectorType");
			HashMap<String, Object> connectorData = (HashMap<String, Object>) connection.get("connectorData");
			HashMap<String, Object> dataSelection = (HashMap<String, Object>) connection.get("dataSelection");
			if (connectorType.toUpperCase().equals("LOCAL")) {
				String engineName = (String) connectorData.get("engineName");
				IEngine engine = Utility.getEngine(engineName);
				for (String table : dataSelection.keySet()) {
					HashMap<String, Object> allColumns = (HashMap<String, Object>) dataSelection.get(table);
					for (String column : allColumns.keySet()) {
						Boolean selectedValue = (Boolean) allColumns.get(column);
						if (selectedValue) {
							if (table.equals(column)) {
								String fileName = engineName + ";" + table + ";";
								String testFilePath = outputFolder + "\\" + fileName + ".txt";
								testFilePath = testFilePath.replace("\\", "/");
								String uri = DomainValues.getConceptURI(table, engine, true);
								List<Object> instances;
								if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
									instances = DomainValues.retrieveCleanConceptValues(uri, engine);
								} else {
									instances = DomainValues.retrieveCleanConceptValues(table, engine);
								}
								encodeInstances(testFilePath, instances);
							} else {
								String fileName = engineName + ";" + table + ";" + column;
								String testFilePath = outputFolder + "\\" + fileName + ".txt";
								testFilePath = testFilePath.replace("\\", "/");
								String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
										+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
								minHashFilePath = minHashFilePath.replace("\\", "/");
								String conceptUri = DomainValues.getConceptURI(table, engine, true);
								String propUri = DomainValues.getPropertyURI(column, table, engine, false);
								List<Object> instances;
								if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
									instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
								} else {
									instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
								}
								encodeInstances(testFilePath, instances);

							}
						}

					}
				}
			} else if (connectorType.toUpperCase().equals("EXTERNAL")) {
				// process if jdbc connection
				String connectionUrl = (String) connectorData.get("connectionString");
				String port = (String) connectorData.get("port");
				String host = (String) connectorData.get("host");
				String schema = (String) connectorData.get("schema");
				String username = (String) connectorData.get("userName");
				String password = (String) connectorData.get("password");
				String newDBName = (String) connectorData.get("databaseName");
				String type = (String) connectorData.get("type");
				Connection con = buildConnection(type, host, port, username, password, schema);

				for (String table : dataSelection.keySet()) {
					HashMap<String, Object> allColumns = (HashMap<String, Object>) dataSelection.get(table);
					for (String column : allColumns.keySet()) {
						Boolean selectedValue = (Boolean) allColumns.get(column);
						if (selectedValue) {
							// build sql query - write only unique values
							StringBuilder sb = new StringBuilder();
							sb.append("SELECT DISTINCT ");
							sb.append(column);
							sb.append(" FROM ");
							sb.append(table);
							sb.append(";");
							String query = sb.toString();

							// execute query against db
							Statement stmt = null;

							try {
								stmt = con.createStatement();
								ResultSet rs = stmt.executeQuery(query);
								String fileName = newDBName + ";" + table + ";" + column;
								String testFilePath = outputFolder + "\\" + fileName + ".txt";
								testFilePath = testFilePath.replace("\\", "/");
								List<Object> instances = Utility.getInstancesFromRs(rs);
								encodeInstances(testFilePath, instances);
								stmt.close();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
					}
				}
				con.close();

			} else if (connectorType.toUpperCase().equals("FILE")) {

				// process csv file reading
				String filePath = (String) connectorData.get("filePath");
				String extension = FilenameUtils.getExtension(filePath);
				if (extension.equals("csv") || extension.equals("txt")) {
					String[] csvFileName = filePath.split("\\\\");
					String fileName = csvFileName[csvFileName.length - 1].replace(".csv", "");
					// read csv into string[]
					char delimiter = ','; // TODO get from user
					CSVReader csv;
					if (delimiter == '\t') {
						csv = new CSVReader(new FileReader(new File(filePath)));
					} else {
						csv = new CSVReader(new FileReader(new File(filePath)));
					}
					List<String[]> rowData = csv.readAll(); // get all rows
					String[] headers = rowData.get(0);
					List<String> selectedCols = new ArrayList<String>();
					for (String col : dataSelection.keySet()) {
						// make a list of selected columns
						HashMap<String, Object> colInfo = (HashMap<String, Object>) dataSelection.get(col);
						for (String cols : colInfo.keySet()) {
							if ((Boolean) colInfo.get(cols) == true) {
								selectedCols.add(cols);
							}
						}
					}

					// iterate through selected columns and only grab those
					// instances where the indices match
					for (String col : selectedCols) {
						// find the index of the selected column in the header
						// array
						int index = -1;
						for (String header : headers) {
							if (header.toUpperCase().equals(col.toUpperCase())) {
								index = Arrays.asList(headers).indexOf(header);
								System.out.println(index);
							}
						}

						// get instance values
						if (index != -1) {
							HashSet<Object> instances = new HashSet<Object>(); // use
																				// hashset
																				// to
																				// get
																				// distinct
																				// values
							for (int j = 0; j < rowData.size(); j++) {
								if (j == 1) {
									continue;
								}

								else {
									instances.add(rowData.get(j)[index]);
								}

							}
							String testFilePath = outputFolder + "\\" + fileName + ";" + col + ".txt";
							testFilePath = testFilePath.replace("\\", "/");
							encodeInstances(testFilePath, instances);
						}

					}

				} else if (extension.equals("xls") || extension.equals("xlsx")) {
					XLFileHelper xl = new XLFileHelper();
					xl.parse(filePath);
					String sheetName = (String) connectorData.get("worksheet");

					// put all row data into a List<String[]>
					List<String[]> rowData = new ArrayList<String[]>();

					String[] row = null;
					while ((row = xl.getNextRow(sheetName)) != null) {
						rowData.add(row);
					} // values

					String[] headers = xl.getHeaders(sheetName);
					List<String> selectedCols = new ArrayList<String>();
					for (String col : dataSelection.keySet()) {
						// make a list of selected columns
						HashMap<String, Object> colInfo = (HashMap<String, Object>) dataSelection.get(col);
						for (String cols : colInfo.keySet()) {
							if ((Boolean) colInfo.get(cols) == true) {
								selectedCols.add(cols);
							}
						}
					}

					for (String col : selectedCols) {
						// find the index of the selected column in the header
						// array
						int index = -1;
						for (String header : headers) {
							if (header.toUpperCase().equals(col.toUpperCase())) {
								index = Arrays.asList(headers).indexOf(header);
								System.out.println(index);
							}
						}

						// get instance values
						if (index != -1) {
							HashSet<Object> instances = new HashSet<Object>();
							for (int j = 0; j < rowData.size(); j++) {

								instances.add(rowData.get(j)[index]);

							}
							String testFilePath = outputFolder + "\\" + sheetName + ";" + col + ".txt";
							testFilePath = testFilePath.replace("\\", "/");
							encodeInstances(testFilePath, instances);
						}

					}

				}

			}

		}

		String baseMatchingFolder = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "XrayCompatibility";
		baseMatchingFolder = baseMatchingFolder.replace("\\", "/");

		// Grab the corpus directory
		String corpusDirectory = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "XrayCompatibility\\Temp\\MatchingRepository";
		corpusDirectory = corpusDirectory.replace("\\", "/");

		// Grab the csv directory Semoss/R/Matching/Temp/rdf
		String baseRDFDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdf";
		baseRDFDirectory = baseRDFDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdf/MatchingCsvs
		String rdfCsvDirectory = baseRDFDirectory + "\\" + Constants.R_MATCHING_CSVS_FOLDER;
		rdfCsvDirectory = rdfCsvDirectory.replace("\\", "/");

		// Grab the prop directory Semoss/R/Matching/Temp/rdf/MatchingProp
		String rdfPropDirectory = baseRDFDirectory + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_MATCHING_PROP_FOLDER;
		rdfPropDirectory = rdfPropDirectory.replace("\\", "/");

		// Semoss/R/Matching/Temp/rdbms
		String rdbmsDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdbms";
		rdbmsDirectory = rdbmsDirectory.replace("\\", "/");

		int nMinhash;
		int nBands;
		int instancesThreshold = 1;
		// TODO get parameters from front end
		double similarityThreshold = -1;
		double candidateThreshold = -1;
		String matchingSameDBR = "FALSE";
		boolean matchingSameDB = false;

		if (parameters != null) {
			Object sim = parameters.get("similarity");
			Double similarity = null;
			if (sim instanceof Integer) {
				similarity = (double) ((Integer) sim).intValue();
			} else {
				similarity = (Double) sim;
			}
			if (similarity != null) {
				similarityThreshold = similarity.doubleValue();
			}
			Object cand = parameters.get("candidate");

			Double candidate = null;

			if (cand instanceof Integer) {
				candidate = (double) ((Integer) cand).intValue();
			} else {
				candidate = (Double) cand;
			}
			if (candidate != null) {
				candidateThreshold = candidate.doubleValue();
			}
			Boolean matchDB = (Boolean) parameters.get("matchSameDb");
			if (matchDB != null) {
				matchingSameDB = matchDB.booleanValue();
			}
		}

		if (similarityThreshold < 0 || similarityThreshold > 1) {
			similarityThreshold = 0.01;
		}

		if (candidateThreshold < 0 || candidateThreshold > 1) {
			candidateThreshold = 0.01;
		}

		// check if user wants to compare columns from the same database
		if (matchingSameDB) {
			matchingSameDBR = "TRUE";
		}

		// set other parameters
		if (candidateThreshold <= 0.03) {
			nMinhash = 3640;
			nBands = 1820;
		} else if (candidateThreshold <= 0.02) {
			nMinhash = 8620;
			nBands = 4310;
		} else if (candidateThreshold <= 0.01) {
			nMinhash = 34480;
			nBands = 17240;
		} else if (candidateThreshold <= 0.05) {
			nMinhash = 1340;
			nBands = 670;
		} else if (candidateThreshold <= 0.1) {
			nMinhash = 400;
			nBands = 200;
		} else if (candidateThreshold <= 0.2) {
			nMinhash = 200;
			nBands = 100;
		} else if (candidateThreshold <= 0.4) {
			nMinhash = 210;
			nBands = 70;
		} else if (candidateThreshold <= 0.5) {
			nMinhash = 200;
			nBands = 50;
		} else {
			nMinhash = 200;
			nBands = 40;
		}

		// Parameters for R script
		String rFrameName = "this.dt.name.is.reserved.for.semantic.matching";

		// Grab the utility script
		String utilityScriptPath = baseMatchingFolder + "\\" + "matching.R";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		runR("library(textreuse)");

		// Source the LSH function from the utility script
		runR("source(\"" + utilityScriptPath + "\");");
		runR(rFrameName + " <- data.frame()");

		// Run locality sensitive hashing to generate matches
		runR(rFrameName + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + corpusDirectory + "\", " + nMinhash + ", "
				+ nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
				+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchingSameDBR + ", \"" + rdbmsDirectory
				+ "\", \"" + metadataFile + "\")");

		// Synchronize from R
		storeVariable("GRID_NAME", rFrameName);
		synchronizeFromR();

		// TODO save to local master?

		// // Persist the data into a database
		// String matchingDbName = "MatchingRDBMSDatabase";
		// IEngine engine = Utility.getEngine(matchingDbName);
		//
		// // Only add to the engine if it is null
		// // TODO gracefully refresh the entire db
		// if (engine == null) {
		// MatchingDB db = new MatchingDB(getBaseFolder());
		// // creates rdf and rdbms dbs
		// // TODO specify dbType if desired
		// String matchingDBType = ImportOptions.DB_TYPE.RDBMS.toString();
		// db.saveDB(matchingDBType);
		//
		// }
		// this.hasReturnData = true;
		return "";
		// return null;
	}

	public String getXrayConfigList() throws JsonGenerationException, JsonMappingException, IOException {
		HashMap<String, Object> configMap = MasterDatabaseUtility.getXrayConfigList();
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		this.hasReturnData = true;
		this.returnData = ow.writeValueAsString(configMap);
		return (String) this.returnData;
	}
	public String getXrayConfigFile(String configFileID) throws JsonGenerationException, JsonMappingException, IOException {
		String configFile = MasterDatabaseUtility.getXrayConfigFile(configFileID);
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		this.hasReturnData = true;
		this.returnData = configFile;
		return (String) this.returnData;
	}
	
	public String getSchemaForLocal(String engineName)
			throws JsonGenerationException, JsonMappingException, IOException {
		IEngine engine = Utility.getEngine(engineName);
		List<String> concepts = DomainValues.getConceptList(engine);
		QueryStruct qs = engine.getDatabaseQueryStruct();
		Map<String, Map<String, List>> relations = qs.getRelations();
		// get relations
		Map<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		//structure is Title = {inner.join={Genre, Studio, Nominated}}
		
		
		
		for(String concept : concepts) {
			concept = DomainValues.determineCleanConceptName(concept, engine);
			if(concept.equals("Concept")) {
				continue;
			}
			//check if concept is in the relationship hashmap, if not just add an empty list 
			List<String> conceptRelations = new ArrayList<String>();
			for (String key : relations.keySet()) {
				if (concept.equalsIgnoreCase(key)) {
					conceptRelations = relations.get(key).get("inner.join"); //TODO check if this changes 
				} 
			}
			relationshipMap.put(concept, conceptRelations);
			
		}
		
		

		// tablename: [{name, type}]
		HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>();

		for (String conceptURI : concepts) {
			String cleanConcept = DomainValues.determineCleanConceptName(conceptURI, engine);
			//ignore default concept value
			if (cleanConcept.equals("Concept")) {
				continue;
			}
			ArrayList<HashMap> allCols = new ArrayList<HashMap>();
			HashMap<String, String> colInfo = new HashMap<String, String>();
			colInfo.put("name", cleanConcept);
			String dataType = engine.getDataTypes(conceptURI);
			if(dataType != null) {
				dataType = IMetaData.convertToDataTypeEnum(dataType).toString();
			}
			else {
				dataType = IMetaData.DATA_TYPES.STRING.toString();
			}
			colInfo.put("type", dataType);
			allCols.add(colInfo);
			List<String> properties = DomainValues.getPropertyList(engine, conceptURI);
			for (String prop : properties) {
				String cleanProp = DomainValues.determineCleanPropertyName(prop, engine);
				HashMap<String, String> propInfo = new HashMap<String, String>();
				propInfo.put("name", cleanProp);
				dataType = engine.getDataTypes(prop);
				if(dataType != null) {
					dataType = IMetaData.convertToDataTypeEnum(dataType).toString();
				}
				else {
					dataType = IMetaData.DATA_TYPES.STRING.toString();
				}
				dataType = IMetaData.convertToDataTypeEnum(dataType).toString();
				propInfo.put("type", dataType);
				allCols.add(propInfo);
			}
			tableDetails.put(cleanConcept, allCols);
		}
		
		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("databaseName", engine.getEngineName());
		ret.put("tables", tableDetails);
		ret.put("relationships", relationshipMap);
		
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		this.hasReturnData = true;
		this.returnData = ow.writeValueAsString(ret);
		return (String) this.returnData;
	}
	
	public String getSchemaForXL(String filePath, String sheetName)
			throws JsonGenerationException, JsonMappingException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		XLFileHelper helper = new XLFileHelper();
		helper.parse(filePath);
		ret.put("databaseName", FilenameUtils.getName(filePath).replace(".", "_"));

		// store the suggested data types
		Map<String, Map<String, String>> dataTypes = new Hashtable<String, Map<String, String>>();
		Map<String, String> sheetDataMap = new LinkedHashMap<String, String>();
		String[] columnHeaders = helper.getHeaders(sheetName);
		String[] predicatedDataTypes = helper.predictRowTypes(sheetName);

		HashMap<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		for (String concept : columnHeaders) {
			relationshipMap.put(concept, new ArrayList<String>());
		}

		ret.put("relationships", relationshipMap);

		dataTypes.put(sheetName, sheetDataMap);

		HashMap<String, HashMap> tableDetails = new HashMap<String, HashMap>();
		for (int i = 0; i < columnHeaders.length; i++) {
			HashMap<String, String> colDetails = new HashMap<String, String>();
			colDetails.put("name", columnHeaders[i]);
			String dataType = Utility.getCleanDataType(predicatedDataTypes[i]);
			colDetails.put("type", dataType);
			tableDetails.put(columnHeaders[i], colDetails);
		}

		ret.put("tables", tableDetails);
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		this.hasReturnData = true;
		this.returnData = ow.writeValueAsString(ret);

		return (String) this.returnData;

	}

	
	
	public String getSchemaForCSV(String filePath) throws JsonGenerationException, JsonMappingException, IOException {
		CSVFileHelper cv = new CSVFileHelper();
		cv.parse(filePath);
		String[] headers = cv.getAllCSVHeaders();
		String[] types = cv.predictTypes();
		
		HashMap<String, Object> ret = new HashMap<String, Object>();
		//generate db name
		String[] parts = filePath.split("\\\\");
		String dbName = parts[parts.length-1].replace(".", "_");
		// C:\\..\\file.csv -> file_csv
		ret.put("databaseName", dbName);
		
		//construct empty relationship map (assuming flat table)
		HashMap<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		for(String concept : headers) {
			relationshipMap.put(concept, new ArrayList<String>()); //return empty list for FE
		}
		
		ret.put("relationships", relationshipMap);
		
		//add column details
		//since it's a flat table we don't need to worry about concept/property relationships
		HashMap<String, HashMap> tableDetails = new HashMap<String, HashMap>();
		for(int i = 0; i < headers.length; i++) {
			HashMap<String, String> colDetails = new HashMap<String, String>();
			colDetails.put("name", headers[i]);
			String dataType = IMetaData.convertToDataTypeEnum(types[i]).toString();
			colDetails.put("type", dataType);
			tableDetails.put(headers[i], colDetails);
		}
		
		ret.put("tables", tableDetails);
		
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		this.hasReturnData = true;
		this.returnData = ow.writeValueAsString(ret);
		return (String) this.returnData;
		
	}
	private void encodeInstances(String filePath, List<Object> instances) {
		String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
		minHashFilePath = minHashFilePath.replace("\\", "/");
		
		StringBuilder rsb = new StringBuilder();
		rsb.append("library(textreuse);");
		rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");

		// construct R dataframe
		String dfName = "df.xray";
		rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
		for (int j = 0; j < instances.size(); j++) {
			rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
			rsb.append("<-");
			if(instances.get(j)==null) {
				rsb.append("\"" + "" + "\"");
			} else {
				rsb.append("\"" + instances.get(j).toString() + "\"");
			} 
			rsb.append(";");

		}
		rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
		runR(rsb.toString());
	}
	
	private void encodeInstances(String filePath, HashSet<Object> instances) {
		String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
		minHashFilePath = minHashFilePath.replace("\\", "/");
		
		StringBuilder rsb = new StringBuilder();
		rsb.append("library(textreuse);");
		rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");

		// construct R dataframe
		String dfName = "df.xray";
		rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
		int j = 0;
		for (Object value : instances) {
			rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
			rsb.append("<-");
			if (value == null) {
				rsb.append("\"" + "" + "\"");
			} else {
				rsb.append("\"" + value.toString() + "\"");
			}
			rsb.append(";");
			j++;

		}
		rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
		runR(rsb.toString());
	}
	
	public String getSchemaForExternal(String type, String host, String port, String username, String password, String schema) throws SQLException  {
		Connection con = null;
		try {
			con = buildConnection(type, host, port, username, password, schema);
			String url = "";

			HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>(); // tablename:
			// [colDetails]
			HashMap<String, ArrayList<HashMap>> relations = new HashMap<String, ArrayList<HashMap>>(); // sub_table:
			// [(obj_table,
			// fromCol,
			// toCol)]

			DatabaseMetaData meta = con.getMetaData();
			ResultSet tables = meta.getTables(null, null, null, new String[] { "TABLE" });
			while (tables.next()) {
				ArrayList<String> primaryKeys = new ArrayList<String>();
				HashMap<String, Object> colDetails = new HashMap<String, Object>(); // name:
				// ,
				// type:
				// ,
				// isPK:
				ArrayList<HashMap> allCols = new ArrayList<HashMap>();
				HashMap<String, String> fkDetails = new HashMap<String, String>();
				ArrayList<HashMap> allRels = new ArrayList<HashMap>();

				String table = tables.getString("table_name");
				System.out.println("Table: " + table);
				ResultSet keys = meta.getPrimaryKeys(null, null, table);
				while (keys.next()) {
					primaryKeys.add(keys.getString("column_name"));

					System.out.println(keys.getString("table_name") + ": " + keys.getString("column_name") + " added.");
				}

				System.out.println("COLUMNS " + primaryKeys);
				keys = meta.getColumns(null, null, table, null);
				while (keys.next()) {
					colDetails = new HashMap<String, Object>();
					colDetails.put("name", keys.getString("column_name"));
					colDetails.put("type", keys.getString("type_name"));
					if (primaryKeys.contains(keys.getString("column_name"))) {
						colDetails.put("isPK", true);
					} else {
						colDetails.put("isPK", false);
					}
					allCols.add(colDetails);

					System.out.println(
							"\t" + keys.getString("column_name") + " (" + keys.getString("type_name") + ") added.");
				}
				tableDetails.put(table, allCols);

				System.out.println("FOREIGN KEYS");
				keys = meta.getExportedKeys(null, null, table);
				while (keys.next()) {
					fkDetails = new HashMap<String, String>();
					fkDetails.put("fromCol", keys.getString("PKCOLUMN_NAME"));
					fkDetails.put("toTable", keys.getString("FKTABLE_NAME"));
					fkDetails.put("toCol", keys.getString("FKCOLUMN_NAME"));
					allRels.add(fkDetails);

					System.out.println(keys.getString("PKTABLE_NAME") + ": " + keys.getString("PKCOLUMN_NAME") + " -> "
							+ keys.getString("FKTABLE_NAME") + ": " + keys.getString("FKCOLUMN_NAME") + " added.");
				}
				relations.put(table, allRels);
			}
			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put("databaseName", con.getCatalog());
			ret.put("tables", tableDetails);
			ret.put("relationships", relations);
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			this.hasReturnData = true;
			this.returnData = ow.writeValueAsString(ret);
			con.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (con != null) {
				con.close();
			}
		}

		return (String) this.returnData;
        
  }

	private Connection buildConnection(String type, String host, String port, String username, String password,
			String schema) throws SQLException, JsonGenerationException, JsonMappingException, IOException {
        Connection con = null;
        String url = "";

		try {
            if (type.equals("MYSQL")) {
                   Class.forName("com.mysql.jdbc.Driver");
                  // Connection URL format:
                  // jdbc:mysql://<hostname>[:port]/<DBname>?user=username&password=pw
                  url = "jdbc:mysql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }
                  con = DriverManager.getConnection(url + "?user=" + username + "&password=" + new String(password));
            } else if (type.equals("Oracle")) {
                   Class.forName("oracle.jdbc.driver.OracleDriver");

                  // Connection URL format:
                  // jdbc:oracle:thin:@<hostname>[:port]/<service or sid>[-schema
                  // name]
                  url = "jdbc:oracle:thin:@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));
            } else if (type.equals("SQL_Server")) {
                   Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

                  // Connection URL format:
                  // jdbc:sqlserver://<hostname>[:port];databaseName=<DBname>
                  url = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));
            } else if (type.equals("DB2")) {
                   Class.forName("com.ibm.db2.jcc.DB2Driver");
                  
                  // Connection URL format:
                  // jdbc:db2://<hostname>[:port]/<databasename>
                  url = "jdbc:db2://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("ASTER_DB")) {
                   Class.forName("com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver");
                  url = "jdbc:ncluster://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("SAP_HANA")) {
                   Class.forName("com.sap.db.jdbc.Driver");
                  url = "jdbc:sap://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));
            } else if (type.equals("MARIA_DB")) {
                   Class.forName("org.mariadb.jdbc.Driver");
                  url = "jdbc:mariadb://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("H2_DB")) {
                  Class.forName("org.h2.Driver");
                  //Local db
                  if(host.contains("C:")) {
                      url = "jdbc:h2:HOST/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);

                  } else {
                  url = "jdbc:h2:tcp://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  }
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }
                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("TERADATA")) {
                   Class.forName("com.teradata.jdbc.TeraDriver");
                  url = "jdbc:teradata://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("POSTGRES")) {
                   Class.forName("org.postgresql.Driver");
                  url = "jdbc:postgresql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("DERBY")) {
                   Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                  url = "jdbc:derby://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("CASSANDRA")) {
                   Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
                  url = "jdbc:cassandra://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            } else if (type.equals("IMPALA")) {
                   Class.forName("com.cloudera.impala.jdbc3.Driver");
                  url = "jdbc:impala://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));
            } else if (type.equals("PHOENIX")) {
                   Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                  url = "jdbc:phoenix:HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
                  if (port != null && !port.isEmpty()) {
                         url = url.replace(":PORT", ":" + port);
                  } else {
                         url = url.replace(":PORT", "");
                  }

                  con = DriverManager.getConnection(url, username, new String(password));

            }

     } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(">>>>>DRIVER NOT FOUND. PLEASE ENSURE YOU HAVE ACCESS TO JDBC DRIVER");
     }
		return con;
	}

}
