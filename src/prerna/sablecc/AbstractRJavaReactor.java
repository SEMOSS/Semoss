package prerna.sablecc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

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
import prerna.om.Insight;
import prerna.poi.main.HeadersException;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
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

	protected abstract void performSplitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace);

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
		initiateDriver(url, "sa");

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

		// make sure R is started
		startR();
		eval(frameName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName
				+ "')));");
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
			Map<String, Set<String>> edgeHash = gridFrame.getEdgeHash();
			Map<String, String> dataTypeMap = new HashMap<String, String>();
			for (String colName : edgeHash.keySet()) {
				if (!dataTypeMap.containsKey(colName)) {
					dataTypeMap.put(colName, Utility.convertDataTypeToString(gridFrame.getDataType(colName)));
				}

				Set<String> otherCols = edgeHash.get(colName);
				for (String otherCol : otherCols) {
					if (!dataTypeMap.containsKey(otherCol)) {
						dataTypeMap.put(otherCol, Utility.convertDataTypeToString(gridFrame.getDataType(otherCol)));
					}
				}
			}

			table.mergeEdgeHash(edgeHash, dataTypeMap);

			StringBuilder selectors = new StringBuilder();
			String[] colSelectors = gridFrame.getColumnHeaders();
			for (int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
				selectors.append(colSelectors[selectIndex]);
				if (selectIndex + 1 < colSelectors.length) {
					selectors.append(", ");
				}
			}

			eval(rVarName + " <-as.data.table(unclass(dbGetQuery(conn,'SELECT " + selectors + " FROM " + tableName
					+ "')));");
			eval("setDT(" + rVarName + ")");

			// modify the headers to be what they used to be because the * will
			// return everything in caps
			String[] currHeaders = getColNames(rVarName, false);
			renameColumn(rVarName, currHeaders, colSelectors, false);
			storeVariable("GRID_NAME", rVarName);
			System.out.println("Completed synchronization as " + rVarName);

			this.dataframe = table;
			this.frameChanged = true;
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

		// if we dont even have a h2frame currently, make a new one
		if (!(dataframe instanceof H2Frame)) {
			determineNewFrameNeeded = true;
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

			Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
			frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);

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
		String script = "drv <- JDBC('" + driver + "', '" + jarLocation + "', identifier.quote='`');"
				+ "conn <- dbConnect(drv, '" + url + "', '" + username + "', '')"; // line
																					// of
																					// R
																					// script
																					// that
																					// connects
																					// to
																					// H2Frame
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
			// TODO: should be able to change the data type dynamically!!!
			// TODO: come back and fix this
			recreateMetadata(frameName);
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
	protected void dropRowsWhereColumnContainsValue(String frameName, String colName, String comparator,
			Object values) {
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
					script.append(frameExpression).append(comparator).append("\"").append(val).append("\"");
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
						script.append(" | ").append(frameExpression).append(comparator).append("\"").append(val)
								.append("\"");
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
		String script = frameName + "$" + colName + " <- tolower(" + frameName + "$" + colName + ")";
		eval(script);
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
		String script = frameName + "$" + colName + " <- toupper(" + frameName + "$" + colName + ")";
		eval(script);
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
		String script = frameName + "$" + colName + " <- str_trim(" + frameName + "$" + colName + ")";
		eval(script);
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
		splitColumn(frameName, columnName, separator, false, true);
	}

	protected void splitColumn(String frameName, String columnName, String separator) {
		splitColumn(frameName, columnName, separator, false, true);
	}

	protected void splitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace) {
		performSplitColumn(frameName, columnName, separator, false, true);
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

	protected void pivot(String columnToPivot, String cols) {
		String frameName = (String) retrieveVariable("GRID_NAME");
		pivot(frameName, true, columnToPivot, cols, null);
	}

	protected void pivot(String frameName, boolean replace, String columnToPivot, String cols) {
		pivot(frameName, true, columnToPivot, cols, null);
	}

	protected void pivot(String frameName, boolean replace, String columnToPivot, String cols,
			String aggregateFunction) {
		// makes the columns and converts them into rows
		// dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		startR();
		String newFrame = Utility.getRandomString(8);

		String keepString = "";
		if (cols != null && cols.length() > 0) {
			String[] columnsToKeep = cols.split(";");
			keepString = ", formula = ";
			for (int colIndex = 0; colIndex < columnsToKeep.length; colIndex++) {
				keepString = keepString + columnsToKeep[colIndex];
				if (colIndex + 1 < columnsToKeep.length)
					keepString = keepString + " + ";
			}
			keepString = keepString + " ~ " + columnToPivot;
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
			newTable = new RDataTable(frameName, (RConnection) retrieveVariable(R_CONN),
					(String) retrieveVariable(R_PORT));
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
		String script = "names(" + frameName + ")[names(" + frameName + ") == \"" + curColName + "\"] = \"" + newColName
				+ "\"";
		System.out.println("Running script : " + script);
		eval(script);
		System.out.println("Successfully modified name = " + curColName + " to now be " + newColName);
		if (checkRTableModified(frameName)) {
			this.dataframe.modifyColumnName(curColName, newColName);
		}
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
		if (orderDirection == null || orderDirection.equalsIgnoreCase("desc")) {
			script = frameName + " <- " + frameName + "[order(rank(" + colName + "))]";
		} else if (orderDirection.equalsIgnoreCase("asc")) {
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
		//if there is only one engine compare to self
		if(engines.length < 2) {
			engines = new String[]{engines[0], engines[0]};
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
					dv.exportInstanceValues(engine, outputFolder, compareProperties);
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
		
		//Semoss/R/Matching/Temp/rdbms
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
		String utilityScriptPath = baseMatchingFolder + "\\" + Constants.R_MATCHING_SCRIPT;
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
				+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", \"" + rdfCsvDirectory + "\", \"" +rdbmsDirectory +"\" )");

		// Synchronize from R
		storeVariable("GRID_NAME", rFrameName);
		synchronizeFromR();

		// Persist the data into a database
		String matchingDbName = "MatchingRDBMSDatabase";
		IEngine engine = Utility.getEngine(matchingDbName);

		// Only add to the engine if it is null
		// TODO gracefully refresh the entire db
		if (engine == null) {
			MatchingDB db = new MatchingDB(getBaseFolder());
			//creates rdf and rdbms dbs
			//TODO specify dbType if desired
			String matchingDBType = "";
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

			Insight insightSource = InsightUtility.createInsight(engineSource);
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

			Insight insightTarget = InsightUtility.createInsight(engineTarget);
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
			String penalty) {
		
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
		List<Object[]> allSourceInstances = new Vector<Object[]>();;
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
			sourceProperties = iEngineSource.getProperties4Concept(conceptUriSource, false);
			Vector<String> cleanPropertiesSource = new Vector<String>();
			for (String propVal : sourceProperties) {
				cleanPropertiesSource.add(DomainValues.determineCleanPropertyName(propVal, iEngineSource));

			}
			sourceHeaders = new Object[1];
			sourceHeaders[0] = conceptSource;
			int col = 0;
			for (int i = 0; i < sourceInstances.length; i++) {
				Object[] row = new Object[1];
				row[0] = sourceInstances[col];
				allSourceInstances.add(row);
				col++;
			}
		}

		// Target
		Vector<Object> targetValues = new Vector<Object>();
		List<Object[]> allTargetInstances =  new Vector<Object[]>();
		Object[] targetHeaders = null;
		String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);
		if (targetIsProperty) {
			String propertyUriTarget = DomainValues.getPropertyURI(propertyTarget, conceptTarget, iEngineTarget, false);
			targetValues = (Vector<Object>) DomainValues.retrieveCleanPropertyValues(conceptUriTarget, propertyUriTarget,
					iEngineTarget);
			for (Object s : targetValues) {
				Object[] row = new Object[1];
				row[0] = s;
				allTargetInstances.add(row);

			}
			targetHeaders = new Object[1];
			targetHeaders[0] = propertyTarget;

		} else {
			targetValues = (Vector<Object>) DomainValues.retrieveCleanConceptValues(conceptUriTarget, iEngineTarget);
			for (Object s : targetValues) {
				Object[] row = new Object[1];
				row[0] = s;
				allTargetInstances.add(row);

			}
			targetHeaders = new Object[1];
			targetHeaders[0] = conceptTarget;
			

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
				sv.append(rowValue[i].toString());
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
				tv.append(rowValue[i].toString());
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
		String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "FuzzyJoinTest\\fuzzy_single_join.r";
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


		method = "jw";
		maxdist = 1 - maxdist;
		String maxDistStr = "" + maxdist;
//		join = "left";
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
//		System.out.println(rCommand.toString());
		runR(rCommand.toString());
		runR(df);
		storeVariable("GRID_NAME", df);
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
	public void runFuzzyJoin2(String match, String join, String method, String maxdist, String gramsize,
			String penalty) {

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

		engineSource = source[0];
		conceptSource = source[1];
		if (source.length > 2) {
			propertySource = source[2];
			if (!propertySource.equals("none") && propertySource.length() > 0) {
				sourceIsProperty = true;
			}
		}

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
		Vector<Object> sourceValues = new Vector<Object>();
		List<String> sourceProperties = new ArrayList<>();
		//Array contains concepts and properties
		ArrayList<Vector<Object>> allSourceInstances = new ArrayList<Vector<Object>>();
		int[] sourceColumnSize;

		String conceptUriSource = DomainValues.getConceptURI(conceptSource, iEngineSource, false);
		if (sourceIsProperty) {
			String propertyUriSource = DomainValues.getPropertyURI(propertySource, conceptSource, iEngineSource, false);
			sourceValues = (Vector<Object>) DomainValues.retrievePropertyValues(conceptUriSource, propertyUriSource,
					iEngineSource);
			sourceColumnSize = new int[] { sourceValues.size() };
			allSourceInstances.add((Vector<Object>) sourceValues);

		} else {
			//retrieve concept values
			sourceValues = DomainValues.retrieveCleanConceptValues(conceptUriSource, iEngineSource);
			allSourceInstances.add((Vector<Object>) sourceValues);
			sourceProperties = iEngineSource.getProperties4Concept(conceptUriSource, false);
			sourceColumnSize = new int[sourceProperties.size() + 1];
			sourceColumnSize[0] = sourceValues.size();
			for (int i = 0; i < sourceProperties.size(); i++) {

				List<Object> sourcePropertyInstances = DomainValues.retrieveCleanPropertyValues(conceptUriSource,
						(String) sourceProperties.get(i), iEngineSource);
				sourceColumnSize[i + 1] = sourcePropertyInstances.size();
				allSourceInstances.add((Vector<Object>) sourcePropertyInstances);
			}

		}

		// Target
		List<Object> targetValues = new ArrayList<Object>();
		List<String> targetProperties = new ArrayList<>();
		ArrayList<Vector<Object>> allTargetInstances = new ArrayList<Vector<Object>>();
		int[] targetColumnSize;

		String conceptUriTarget = DomainValues.getConceptURI(conceptTarget, iEngineTarget, false);
		if (targetIsProperty) {
			String propertyUriTarget = DomainValues.getPropertyURI(propertyTarget, conceptTarget, iEngineTarget, false);
			targetValues = DomainValues.retrievePropertyValues(conceptUriTarget, propertyUriTarget, iEngineTarget);
			targetColumnSize = new int[] { targetValues.size() };
			allTargetInstances.add((Vector<Object>) targetValues);
		} else {
			// targetValues =
			// DomainValues.retrieveConceptValues(conceptUriTarget,
			// iEngineTarget);
			targetValues = DomainValues.retrieveCleanConceptValues(conceptUriTarget, iEngineTarget);
			allTargetInstances.add((Vector<Object>) targetValues);
			targetProperties = iEngineTarget.getProperties4Concept(conceptUriTarget, false);
			targetColumnSize = new int[targetProperties.size() + 1];
			targetColumnSize[0] = sourceValues.size();
			for (int i = 0; i < targetProperties.size(); i++) {
				List<Object> targetPropertyInstances = DomainValues.retrieveCleanPropertyValues(conceptUriTarget,
						(String) targetProperties.get(i), iEngineTarget);
				targetColumnSize[i + 1] = targetPropertyInstances.size();
				allTargetInstances.add((Vector<Object>) targetPropertyInstances);
			}
		}

		String filePathSource = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoin\\Temp\\sourceDataFrame.csv";
		filePathSource = filePathSource.replace("\\", "/");

		String filePathTarget = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
				+ "FuzzyJoin\\Temp\\targetDataFrame.csv";
		filePathTarget = filePathTarget.replace("\\", "/");

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
			// TODO test this csv writer change for target after or add new method in domain values
			// write source values to csv
			PrintWriter sv = new PrintWriter(new File(filePathSource));
			StringBuilder ssb = new StringBuilder();
			// csv headers
			ssb.append(sourceHeader);
			// property headers
			for (int i = 0; i < sourceProperties.size(); i++) {
				ssb.append("," + DomainValues.determineCleanPropertyName((sourceProperties.get(i)), iEngineSource));
			}
			ssb.append("\n");

			List b = Arrays.asList(ArrayUtils.toObject(sourceColumnSize));
			int maxRow = (int) Collections.max(b);
			for (int row = 0; row < maxRow; row++) {
				int columnIndex = -1;
				for (int i = 0; i < allSourceInstances.size(); i++) {
					columnIndex++;
					Vector<Object> col = allSourceInstances.get(i);
					String sourceInstance = "";
					if (row < col.size()) {

						sourceInstance = col.get(row).toString();
					}
					ssb.append(sourceInstance);
					if (columnIndex < sourceColumnSize.length - 1) {
						ssb.append(",");
					}
				}
				if (row < maxRow - 1) {
					ssb.append("\n");
				}
			}
			sv.write(ssb.toString());
			sv.close();

			// write target values to csv
			PrintWriter tv = new PrintWriter(new File(filePathTarget));
			StringBuilder tsb = new StringBuilder();
			// csv headers
			tsb.append(targetHeader);
			// property headers
			for (int i = 0; i < targetProperties.size(); i++) {
				tsb.append(" \t " + DomainValues.determineCleanPropertyName((targetProperties.get(i)), iEngineTarget));
			}
			tsb.append("\n");

			List c = Arrays.asList(ArrayUtils.toObject(targetColumnSize));
			int maxRowTarget = (int) Collections.max(c);
			for (int row = 0; row < maxRowTarget; row++) {
				int columnIndex = -1;
				for (int i = 0; i < allTargetInstances.size(); i++) {
					columnIndex++;
					Vector<Object> col = allTargetInstances.get(i);
					String targetInstance = "";
					if (row < col.size()) {
						// TODO clean?????
						targetInstance = col.get(row).toString();
					}
					tsb.append(targetInstance);
					if (columnIndex < targetColumnSize.length - 1) {
						tsb.append(" \t");
					}
				}
				tsb.append("\n");
			}
			tv.write(tsb.toString());
			tv.close();

			/*
			 * for source and target, change csv -> txt, change delimiter to
			 * tab, change r script to read table
			 */

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// Run Fuzzy Matching in R
//		String utilityScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "FuzzyJoin\\fuzzy_join.r";
//		utilityScriptPath = utilityScriptPath.replace("\\", "/");
//
//		runR("library(fuzzyjoin)");
//		runR("source(\"" + utilityScriptPath + "\");");
//
//		String df1 = "this.df.name.is.reserved.for.fuzzy.join.source";
//		String df2 = "this.df.name.is.reserved.for.fuzzy.join.target";
//
//		StringBuilder sourceRead = new StringBuilder();
//		sourceRead.append(df1 + "<-read.table(\"" + filePathSource + "\"");
//		sourceRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
//				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");
//
//		StringBuilder targetRead = new StringBuilder();
//		targetRead.append(df2 + "<-read.table(\"" + filePathTarget + "\"");
//		targetRead.append(", header = TRUE, sep = \"\\t\", quote = \"\", na.strings = \"\", "
//				+ "check.names = FALSE, strip.white = TRUE, comment.char = \"\", fill = TRUE)");
//
//		String df = "this.df.name.is.reserved.for.fuzzy.join.output";
//		runR(sourceRead.toString());
//		runR(targetRead.toString());
//
//		// build the R command
//		StringBuilder rCommand = new StringBuilder();
//		rCommand.append(df);
//		rCommand.append("<-fuzzy_join(");
//		rCommand.append(df1 + ",");
//		rCommand.append(df2 + ",");
//		// TODO: expand this to select on any column, right now assumes the first column
//		rCommand.append("\"" + sourceHeader + "\"" + ",");
//		rCommand.append("\"" + targetHeader + "\"" + ",");
//		rCommand.append("\"" + join + "\"" + ",");
//		rCommand.append(maxdist + ",");
//		rCommand.append("method=" + "\"" + method + "\"" + ",");
//		rCommand.append("q=" + gramsize + ",");
//		rCommand.append("p=" + penalty + ")");
//		System.out.println(rCommand.toString());
//		runR(rCommand.toString());
//		runR(df);
//		storeVariable("GRID_NAME", df);
//		synchronizeFromR();
	}

}
