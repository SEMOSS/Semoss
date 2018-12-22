package prerna.sablecc2.reactor.runtime;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.ICache;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.r.RSingleton;
import prerna.poi.main.HeadersException;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.sablecc2.reactor.imports.ImportSizeRetrictions;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.sablecc2.reactor.imports.RImporter;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractBaseRClass extends AbstractJavaReactorBaseClass {

	public static final String R_CONN = "R_CONN";
	public static final String R_PORT = "R_PORT";
	public static final String R_ENGINE = "R_ENGINE";
	public static final String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";

	private static long counter = 0;

	protected AbstractRJavaTranslator rJavaTranslator;

	/**
	 * This method is used to set the rJavaTranslator
	 * @param rJavaTranslator
	 */
	public void setRJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
		try {
			this.rJavaTranslator.startR();
		} catch(Exception e) {
			logger.info(e.getMessage());
		}
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	/////////////////// Abstract R Methods /////////////////////

	/**
	 * Reconnect the main R server port
	 * @param port
	 */
	public void reconnectR(int port) {
		RSingleton.getConnection(port);
	}
	
	public void initR(int port) {
		RSingleton.getConnection(port);
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////////// R Methods /////////////////////////

	public void runR(String script) {
		System.out.println("R output: \n" + this.rJavaTranslator.runRAndReturnOutput(script));
	}
	
	protected void recreateMetadata(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = this.rJavaTranslator.getColumns(frameName);
		String[] colTypes = this.rJavaTranslator.getColumnTypes(frameName);

		RDataTable newTable = new RDataTable(this.rJavaTranslator, frameName);
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(newTable, colNames, colTypes, frameName);
		this.nounMetaOutput.add(new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
		this.insight.setDataMaker(newTable);
	}
	
	/**
	 * Shift the dataframe into R with a default name
	 */
	public void synchronizeToR() {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame) {
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
		this.logger.info("Starting to install package " + packageName + "... ");
		this.rJavaTranslator.executeEmptyR("install.packages('" + packageName + "', repos='http://cran.us.r-project.org');");
		this.logger.info("Succesfully installed package " + packageName);
		System.out.println("Succesfully installed package " + packageName);
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
	private void synchronizeGridToR(String frameName) {
		long start = java.lang.System.currentTimeMillis();
		logger.info("Synchronizing H2Frame to R data.table...");
		H2Frame gridFrame = (H2Frame) dataframe;
		Map<String, SemossDataType> origDataTypes = gridFrame.getMetaData().getHeaderToTypeMap();
		
		// note : do not use * since R will not preserve the column order
		StringBuilder selectors = new StringBuilder();
		String[] colSelectors = gridFrame.getColumnHeaders();
		for (int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
			String colSelector = colSelectors[selectIndex];
			selectors.append(colSelector);
			if (selectIndex + 1 < colSelectors.length) {
				selectors.append(", ");
			}
		}

		// we'll write to CSV and load into data.table to avoid rJava setup
		final String sep = java.lang.System.getProperty("file.separator");
		String random = Utility.getRandomString(10);
		String outputLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/") + sep + "R" + sep + "Temp" + sep + "output" + random + ".tsv";
		ResultSet rs = null;
		try {
			rs = gridFrame.execQuery("CALL CSVWRITE("
					+ "'" + outputLocation + "', "
					+ "'SELECT " + selectors + " FROM " + gridFrame.getTableName() + "', "
					+ "STRINGDECODE('charset=UTF-8 fieldDelimiter=\"\" fieldSeparator=\t null=\"NA\"')"
					+ ");");
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		this.rJavaTranslator.executeEmptyR("library(data.table);");
		this.rJavaTranslator.executeEmptyR(frameName + " <- fread(\"" + outputLocation + "\", sep=\"\t\");");
		File f = new File(outputLocation);
		f.delete();
		this.rJavaTranslator.executeEmptyR("setDT(" + frameName + ")");

		// modify the headers to be what they used to be because the query
		// return everything in
		// all upper case which may not be accurate
		String[] currHeaders = this.rJavaTranslator.getColumns(frameName);
		renameColumn(frameName, currHeaders, colSelectors, false);
		// and also redo the types
		String[] types = this.rJavaTranslator.getColumnTypes(frameName);
		recalculateTypes(frameName, origDataTypes, colSelectors, types);
		
		storeVariable("GRID_NAME", new NounMetadata(frameName, PixelDataType.CONST_STRING));
		System.out.println("Completed synchronization as " + frameName);
		long end = java.lang.System.currentTimeMillis();
		logger.info("Done synchroizing to R data.table...");
		logger.debug("Time to finish synchronizing to R data.table " + (end-start) + "ms");
	}

	/**
	 * Determine the correct types when going from tsv to R
	 * @param origDataTypes
	 * @param colSelectors
	 * @param types
	 */
	private void recalculateTypes(String tableName, Map<String, SemossDataType> origDataTypes, String[] colSelectors, String[] types) {
		Map<String, SemossDataType> dataTypes = new HashMap<String, SemossDataType>();
		for(String col : origDataTypes.keySet()) {
			if(col.contains("__")) {
				dataTypes.put(col.split("__")[1], origDataTypes.get(col));
			} else {
				dataTypes.put(col, origDataTypes.get(col));
			}
		}
		
		int numCols = colSelectors.length;
		for(int i = 0; i < numCols; i++) {
			String col = colSelectors[i];
			String type = types[i];
			SemossDataType origType = dataTypes.get(col);
			SemossDataType rType = SemossDataType.convertStringToDataType(type);
			this.rJavaTranslator.changeColumnType(tableName, col, origType, rType);
		}
	}

	/**
	 * Synchronize current H2Frame into a R Data Table Frame
	 * 
	 * @param rVarName
	 */
	protected void synchronizeGridToRDataTable(String rVarName) {
		// defualt will be to replace the existing frame on the insight
		// with the new R Data Table we are about to make
		synchronizeGridToRDataTable(rVarName, true);
	}

	protected RDataTable synchronizeGridToRDataTable(String rVarName, boolean replaceDefaultInsightFrame) {
		if(rVarName == null || rVarName.isEmpty()) {
			rVarName = getDefaultName();
		}
		// if there is a current r serve session
		// use that for the frame so we have all the other variables
		RDataTable table = new RDataTable(this.rJavaTranslator, rVarName);

		// if currently no frame
		// return empty one
		if(dataframe == null) {
			return table;
		}
		
		table.setUserId(dataframe.getUserId());
		if (dataframe instanceof H2Frame) {
			H2Frame gridFrame = (H2Frame) dataframe;
			String tableName = gridFrame.getBuilder().getTableName();
			synchronizeGridToR(rVarName);
			
			// now that we have created the frame
			// we need to set the metadata for the frame
			OwlTemporalEngineMeta newMeta = gridFrame.getMetaData().copy();
			newMeta.modifyVertexName(tableName, rVarName);
			table.setMetaData(newMeta);

			if(replaceDefaultInsightFrame) {
				gridFrame.close();
			}
			
		} else if(dataframe  instanceof RDataTable){
			// ughhh... why are you calling this?
			// i will just change the r var name
			table.executeRScript(rVarName + " <- " + ((RDataTable) dataframe).getTableName());
			table.setTableName(rVarName);
			table.setMetaData(dataframe.getMetaData());
			// also, dont forget to update the metadata
			table.getMetaData().modifyVertexName(((RDataTable) dataframe).getTableName(), rVarName);

		} else if(dataframe instanceof NativeFrame || dataframe instanceof PandasFrame) {
			IRawSelectWrapper it = dataframe.iterator();
			if(!ImportSizeRetrictions.sizeWithinLimit(it.getNumRecords())) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			
			SelectQueryStruct qs = dataframe.getMetaData().getFlatTableQs();
			qs.setFrame(dataframe);
			RImporter importer = new RImporter(table, qs, it);
			importer.insertData();

		} else {
			throw new IllegalArgumentException("Frame must be a grid or a native frame in order to move into R for 'Clean Data' and 'Analyze Data' widgets");
		}
		// now we return the data
		this.nounMetaOutput.add(new NounMetadata(table, PixelDataType.FRAME, PixelOperationType.FRAME));
		if(replaceDefaultInsightFrame) {
			this.insight.setDataMaker(table);
			// need to clean up variables if we are dropping the frame
			VarStore varStore = this.insight.getVarStore();
			Set<String> curReferences = varStore.getAllAliasForObjectReference(dataframe);
			// switch to the new frame
			for(String reference : curReferences) {
				varStore.put(reference, new NounMetadata(table, PixelDataType.FRAME));
			}
		}
		
		// move over the filters
		table.setFilter(dataframe.getFrameFilters());

		Map<String, SemossDataType> types = table.getMetaData().getHeaderToTypeMap();
		String frameName = table.getTableName();
		// change r dataTypes such as dates, logicals, etc to be displayed as strings
		StringBuilder dataTypeConversion = new StringBuilder();
		for (String colName : types.keySet()) {
			SemossDataType smssType = types.get(colName);
			if (colName.contains("__")) {
				String[] split = colName.split("__");
				colName = split[1];
			}
			if (smssType == SemossDataType.INT || smssType == SemossDataType.DOUBLE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToNumeric(frameName, colName) + ";");
			}
			if (smssType == SemossDataType.STRING || smssType == SemossDataType.DATE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToCharacter(frameName, colName) + ";");
			}
		}
		if (dataTypeConversion.toString().length() > 0) {
			this.rJavaTranslator.runR(dataTypeConversion.toString());
		}
		
		return table;
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
		String[] colNames = this.rJavaTranslator.getColumns(frameName);
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
		colNames = cleanColNames.toArray(new String[]{});
		String[] colTypes = this.rJavaTranslator.getColumns(frameName);

		// generate the QS
		// set the column names and types
		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSelectorsAndTypes(colNames, colTypes);

		/*
		 * logic to determine where we are adding this data... 1) First, make
		 * sure the existing frame is a grid -> If it is not a grid, we already
		 * know we need to make a new h2frame 2) Second, if it is a grid, check
		 * the meta data and see if it has changed -> if it has changed, we need
		 * to make a new h2frame 3) Regardless of #2 -> user can decide what
		 * they want to create a new frame even if the meta data hasn't changed
		 */

		H2Frame frameToUse = null;
		boolean frameIsH2 = false;
		String schemaName = null;
		String tableName = null;
		boolean determineNewFrameNeeded = false;
		boolean syncExistingRMetadata = false;
		OwlTemporalEngineMeta newMeta = null;

		// if we dont even have a h2frame currently, make a new one
		if (!(dataframe instanceof H2Frame)) {
			determineNewFrameNeeded = true;
			if(dataframe instanceof RDataTable && ((RDataTable) dataframe).getTableName().equals(frameName)) {
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

		if (!overrideExistingTable || determineNewFrameNeeded) {
			frameToUse = new H2Frame();
			tableName = frameToUse.getTableName();

			// if we can use the existing metadata, use it
			if(syncExistingRMetadata) {
				newMeta = this.dataframe.getMetaData().copy();
				newMeta.modifyVertexName(frameName, frameToUse.getTableName());
			} 
			
			// set the correct schema in the new frame
			// drop the existing table
			if (frameIsH2) {
				frameToUse.setUserId(schemaName);
				((H2Frame) dataframe).close();
			} else {
				// this is set when we set the original dataframe
				// within the reactor
				frameToUse.setUserId(this.insight.getUserId());
			}
			
			
			
			//			else {
			//				// create a prim key one
			//				Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
			//				frameToUse.mergeEdgeHash(edgeHash, dataTypeMapStr);
			//			}
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
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\" + DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		this.rJavaTranslator.executeEmptyR("fwrite(" + frameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		qs.setFilePath(tempFileLocation);
		H2Importer importer = new H2Importer(frameToUse, qs);
		if(syncExistingRMetadata) {
			importer.insertData(newMeta);
		} else {
			// importer will create the necessary meta information 
			importer.insertData();

		}

		//		// keep track of in-mem vs on-disk frames
		//		int limitSizeInt = RdbmsFrameUtility.getLimitSize();
		//		if (dataIterator.numberRowsOverLimit(limitSizeInt)) {
		//			frameToUse.convertToOnDiskFrame(null);
		//		}
		//
		//		// now that we know if we are adding to disk vs mem
		//		// iterate through and add all the data
		//		frameToUse.addRowsViaIterator(dataIterator, dataTypeMap);
		//		dataIterator.deleteFile();

		System.out.println("Table Synchronized as " + tableName);
		// override frame references & table name reference
		if(overrideExistingTable) {
			this.nounMetaOutput.add(new NounMetadata(frameToUse, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
			this.insight.setDataMaker(frameToUse);
		} else {
			this.nounMetaOutput.add(new NounMetadata(frameToUse, PixelDataType.FRAME));
		}
	}

	/**
	 * Synchronize a R data table into a H2Frame
	 * 
	 * @param frameName
	 * @param overrideExistingTable
	 */
	protected void synchronizeGridFromRDataTable(String frameName) {
		synchronizeGridFromR(frameName, true);
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
		this.rJavaTranslator.executeEmptyR(script);

		if (print) {
			System.out.println("Running script : " + script);
			System.out.println("Successfully modified old names = " + Arrays.toString(oldNames) + " to new names " + Arrays.toString(newNames));
		}
		if (checkRTableModified(frameName)) {
			// FE passes the column name
			// but meta will still be table __ column
			for (i = 0; i < size; i++) {
				this.dataframe.getMetaData().modifyPropertyName(frameName + "__" + oldNames[i], frameName, frameName + "__" + newNames[i]);
			}
			this.dataframe.syncHeaders();
		}
	}

	protected boolean checkRTableModified(String frameName) {
		if (this.dataframe instanceof RDataTable) {
			String tableVarName = ((RDataTable) this.dataframe).getTableName();
			if (frameName.equals(tableVarName)) {
				this.dataframe.updateDataId();
				this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE));
				return true;
			}
		}
		return false;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////// Tinker R Methods //////////////////////

	protected void synchronizeGraphToR() {
		String defaultName = getDefaultName();
		synchronizeGraphToR(defaultName);
	}

	/**
	 * Get the current working directory of the R session
	 */
	protected String getWd() {
		return this.rJavaTranslator.getString("getwd()");
	}
	
	protected void synchronizeGraphToR(String rVarName) {
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		String wd = baseFolder + "/" + randomDir;
		synchronizeGraphToR(rVarName, wd);
	}

	private void synchronizeGraphToR(String graphName, String wd) {
		java.io.File file = new File(wd);
		String curWd = null;
		try {
			logger.info("Trying to start R.. ");
			logger.info("Successfully started R");

			// get the current directory
			// we need to switch out of this to write the graph file
			// but want to go back to this original one
			curWd = getWd();

			// create this directory
			file.mkdir();
			String fileName = writeGraph(wd);

			wd = wd.replace("\\", "/");

			// set the working directory
			this.rJavaTranslator.executeEmptyR("setwd(\"" + wd + "\")");
			// load the library
			Object ret = this.rJavaTranslator.executeR("library(\"igraph\");");
			if (ret == null) {
				ICache.deleteFolder(wd);
				throw new ClassNotFoundException("Package igraph could not be found!");
			}
			String loadGraphScript = graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			this.rJavaTranslator.executeEmptyR(loadGraphScript);

			System.out.println("Successfully synchronized, your graph is now available as " + graphName);
			// store the graph name for future use
			storeVariable("GRAPH_NAME", new NounMetadata(graphName, PixelDataType.CONST_STRING));

			// store the directories used for the iGraph
			List<String> graphLocs = new Vector<String>();
			if (retrieveVariable(R_GRAQH_FOLDERS) != null) {
				graphLocs = (List<String>) retrieveVariable(R_GRAQH_FOLDERS);
			}
			graphLocs.add(wd);
			storeVariable(R_GRAQH_FOLDERS, new NounMetadata(graphLocs, PixelDataType.CONST_STRING));
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(
					"ERROR ::: Could not convert TinkerFrame into iGraph.\nPlease make sure iGraph package is installed.");
		} finally {
			// reset back to the original wd
			if (curWd != null) {
				this.rJavaTranslator.executeEmptyR("setwd(\"" + curWd + "\")");
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
				this.rJavaTranslator.executeEmptyR(graphName + " <- delete_vertices(" + graphName + ", V(" + graphName + ")[vertex_attr(" + graphName
						+ ", \"" + TinkerFrame.TINKER_ID + "\") == \"" + name + "\"])");
			} catch (Exception ex) {
				java.lang.System.out.println("ERROR ::: Could not delete node = " + name);
				ex.printStackTrace();
			}
		}
	}

	public void key() {
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		String names = "";
		// get the articulation points
		int [] vertices = this.rJavaTranslator.getIntArray("articulation.points(" + graphName + ")");
		// now for each vertex get the name
		Hashtable <String, String> dataHash = new Hashtable<String, String>();
		for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++)
		{
			String output = this.rJavaTranslator.getString("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\", " + vertices[vertIndex] + ")");
			String [] typeData = output.split(":");
			String typeOutput = "";
			if(dataHash.containsKey(typeData[0]))
				typeOutput = dataHash.get(typeData[0]);
			typeOutput = typeOutput + "  " + typeData[1];
			dataHash.put(typeData[0], typeOutput);
		}

		Enumeration <String> keys = dataHash.keys();
		while(keys.hasMoreElements()) {
			String thisKey = keys.nextElement();
			names = names + thisKey + " : " + dataHash.get(thisKey) + "\n";
		}
		System.out.println(" Key Nodes \n " + names);
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
}
