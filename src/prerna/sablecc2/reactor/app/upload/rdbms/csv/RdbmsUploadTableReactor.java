package prerna.sablecc2.reactor.app.upload.rdbms.csv;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import cern.colt.Arrays;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RdbmsUploadTableReactor extends AbstractUploadFileReactor {

	/*
	 * There are quite a few things that we need
	 * 1) app -> name of the app to create
	 * 1) filePath -> string containing the path of the file
	 * 2) delimiter -> delimiter for the file
	 * 3) dataTypes -> map of the header to the type, this will contain the original headers we send to FE
	 * 4) newHeaders -> map containing old header to new headers for the csv file
	 * 5) additionalTypes -> map containing header to an additional type specification
	 * 						additional inputs would be {header : currency, header : date_format, ... }
	 * 6) clean -> boolean if we should clean up the strings before insertion, default is true
	 * TODO: 7) deduplicate -> boolean if we should remove duplicate rows in the relational database
	 * 8) existing -> boolean if we should add to an existing app, defualt is false
	 */
	
	private CSVFileHelper helper = null;

	public RdbmsUploadTableReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH, 
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.DELIMITER, 
				UploadInputUtility.DATA_TYPE_MAP, 
				UploadInputUtility.NEW_HEADERS, 
				UploadInputUtility.ADDITIONAL_DATA_TYPES,
				UploadInputUtility.CLEAN_STRING_VALUES, 
				UploadInputUtility.REMOVE_DUPLICATE_ROWS,
				UploadInputUtility.REPLACE_EXISTING
		};
	}

	/**
	 * Make a new app with the file data
	 * @param newAppName
	 * @param filePath
	 */
	@Override
	public void generateNewApp(User user, final String newAppName, final String filePath) throws Exception {
		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make owl
		 * 3) make temporary smss
		 * 4) make engine class
		 * 5) load actual data
		 * 6) load owl metadata
		 * 7) load default insights
		 * 8) add to localmaster and solr
		 */

		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = UploadInputUtility.getCsvDataTypeMap(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypeMap = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		String tableName = UploadInputUtility.getTableName(this.store, this.insight);
		String uniqueColumnName = UploadInputUtility.getUniqueColumn(this.store, this.insight);
		final boolean clean = UploadInputUtility.getClean(this.store);
		final boolean replace = UploadInputUtility.getReplace(this.store);

		// now that I have everything, let us go through and insert

		// start by validation
		int stepCounter = 1;
		logger.info("Generate new app database");
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.appId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.createTemporaryRdbmsSmss(this.appId, newAppName, owlFile, "H2_DB", null);
		DIHelper.getInstance().getCoreProp().setProperty(this.appId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		this.engine = new RDBMSNativeEngine();
		this.engine.setEngineId(this.appId);
		this.engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(this.tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		this.engine.setProp(props);
		this.engine.openDB(null);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start loading data..");
		logger.info("Parsing file metadata...");

		String fileName = FilenameUtils.getBaseName(filePath);
		if (fileName.contains("_____UNIQUE")) {
			// ... yeah, this is not intuitive at all,
			// but I add a timestamp at the end to make sure every file is unique
			// but i want to remove it so things are "pretty"
			fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
		}

		// if user doesn't input table name use filename, else use inputted name 
		if (tableName == null) {
			tableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
		} else {
			tableName = RDBMSEngineCreationHelper.cleanTableName(tableName).toUpperCase();
		}

		// if user defines unique column name set that if not generate one
		// TODO: add change for false values once we want to enable that
		String uniqueRowId = uniqueColumnName.equalsIgnoreCase("true") ? tableName + RdbmsUploadReactorUtility.UNIQUE_ROW_ID: uniqueColumnName;

		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		// parse the information
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info("Done parsing file metadata");

		logger.info("Create table...");
		// NOTE ::: SQL_TYPES will have the added unique row id at index 0
		String[] sqlTypes = null;
		try {
			sqlTypes = RdbmsUploadReactorUtility.createNewTable(this.engine, tableName, uniqueRowId, headers, types, replace);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(new NounMetadata("Error occured during upload", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		logger.info("Done create table");
		bulkInsertFile(this.engine, this.helper, tableName, headers, types, additionalTypes, clean);
		RdbmsUploadReactorUtility.addIndex(this.engine, tableName, uniqueRowId);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start generating engine metadata");
		Owler owler = new Owler(owlFile.getAbsolutePath(), ENGINE_TYPE.RDBMS);
		RdbmsUploadReactorUtility.generateTableMetadata(owler, tableName, uniqueRowId, headers, sqlTypes, additionalTypes);
		UploadUtilities.insertFlatOwlMetadata(owler, tableName, headers, UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
		owler.commit();
		owler.export();
		this.engine.setOWL(owlFile.getPath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start generating default app insights");
		RDBMSNativeEngine insightDatabase = UploadUtilities.generateInsightsDatabase(this.appId, newAppName);
		UploadUtilities.addExploreInstanceInsight(this.appId, newAppName, insightDatabase);
		UploadUtilities.addInsightUsageStats(this.appId, newAppName, insightDatabase);
		UploadUtilities.addGridDeltaInsight(this.appId, newAppName, insightDatabase);
		UploadUtilities.addInsertFormInsight(this.appId, newAppName, insightDatabase, owler, this.helper.orderHeadersToGet(headers));
		UploadUtilities.addAuditModificationView(this.appId, newAppName, insightDatabase);
		UploadUtilities.addAuditTimelineView(this.appId, newAppName, insightDatabase);
		this.engine.setInsightDatabase(insightDatabase);
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, owler);
		logger.info(stepCounter + ". Complete");
	}

	/**
	 * Add the data into an existing rdbms engine
	 * @param appId
	 * @param filePath
	 * @throws Exception 
	 */
	@Override
	public void addToExistingApp(String filePath) throws Exception {
		if (!(this.engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}

		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = UploadInputUtility.getCsvDataTypeMap(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypeMap = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		final boolean clean = UploadInputUtility.getClean(this.store);
		final boolean replace = UploadInputUtility.getReplace(this.store);

		int stepCounter = 1;
		logger.info(stepCounter + ". Parsing file metadata...");
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info(stepCounter + ". Done parsing file metadata");
		stepCounter++;

		logger.info(stepCounter + ". Get existing database schema...");
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(this.engine);
		logger.info(stepCounter + ". Done getting existing database schema");
		stepCounter++;

		logger.info("Determine if we can add into an existing table or make new table...");
		String tableToInsertInto = determineExistingTableToInsert(existingRDBMSStructure, headers, types);
		if (tableToInsertInto == null) {
			logger.info("Could not find existing table to insert into");
			logger.info(stepCounter + ". Create table...");
			
			String fileName = FilenameUtils.getBaseName(filePath);
			if (fileName.contains("_____UNIQUE")) {
				// ... yeah, this is not intuitive at all,
				// but I add a timestamp at the end to make sure every file is unique
				// but i want to remove it so things are "pretty"
				fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
			}
			tableToInsertInto = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
			
			String uniqueRowId = tableToInsertInto + RdbmsUploadReactorUtility.UNIQUE_ROW_ID;
			// NOTE ::: SQL_TYPES will have the added unique row id at index 0
			String[] sqlTypes = null;
			try {
				sqlTypes = RdbmsUploadReactorUtility.createNewTable(this.engine, tableToInsertInto, uniqueRowId, headers, types, replace);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(new NounMetadata("Error occured during upload", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			logger.info("Done create table");
			bulkInsertFile(this.engine, this.helper, tableToInsertInto, headers, types, additionalTypes, clean);
			RdbmsUploadReactorUtility.addIndex(this.engine, tableToInsertInto, uniqueRowId);
			logger.info(stepCounter + ". Complete");
			stepCounter++;

			logger.info(stepCounter + ". Start generating engine metadata...");
			Owler owler = new Owler(this.engine);
			RdbmsUploadReactorUtility.generateTableMetadata(owler, tableToInsertInto, uniqueRowId, headers, sqlTypes, additionalTypes);
			UploadUtilities.insertFlatOwlMetadata(owler, tableToInsertInto, headers, UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
			owler.commit();
			owler.export();
			this.engine.setOWL(this.engine.getOWL());
			logger.info(stepCounter + ". Complete");
			stepCounter++;

			logger.info(stepCounter + ". Start generating default app insights");
			Set<String> newTableSet = new HashSet<String>();
			newTableSet.add(tableToInsertInto);
			RDBMSEngineCreationHelper.insertNewTablesAsInsights(this.engine, owler, newTableSet);
			logger.info(stepCounter + ". Complete");
		} else {
			logger.info("Found table " + Utility.cleanLogString(tableToInsertInto) + " that holds similar data! Will insert into this table");
			bulkInsertFile(this.engine, this.helper, tableToInsertInto, headers, types, additionalTypes, clean);
		}
	}
	
	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}

	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Processing actually happens here
	 * 
	 */

	/**
	 * Perform the insertion of data into the table
	 * @param engine
	 * @param helper
	 * @param TABLE_NAME
	 * @param headers
	 * @param types
	 * @param additionalTypes
	 * @param clean
	 * @param logger
	 * @throws IOException
	 */
	private void bulkInsertFile(IEngine engine, CSVFileHelper helper, final String TABLE_NAME,
			String[] headers, SemossDataType[] types, String[] additionalTypes, 
			boolean clean) throws IOException {

		// now we need to loop through the csv data and cast to the appropriate type and insert
		// let us be smart about this and use a PreparedStatement for bulk insert
		// get the bulk statement

		// the prepared statement requires the table name and then the list of columns
		Object[] getPreparedStatementArgs = new Object[headers.length+1];
		getPreparedStatementArgs[0] = TABLE_NAME;
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			getPreparedStatementArgs[headerIndex+1] = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
		}
		PreparedStatement ps = (PreparedStatement) engine.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);

		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;

		logger.info("Start inserting data into table");
		// we loop through every row of the csv
		String[] nextRow = null;
		try {
			while ((nextRow = helper.getNextRow()) != null) {
				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					// nulls get added as null
					// not interesting...
					if (nextRow[colIndex] == null) {
						ps.setObject(colIndex + 1, null);
						continue;
					}

					// yay, actual data
					SemossDataType type = types[colIndex];
					// strings
					if (type == SemossDataType.STRING || type == SemossDataType.FACTOR) {
						String value = null;
						if (clean) {
							value = Utility.cleanString(nextRow[colIndex], false);
						} else {
							value = nextRow[colIndex] + "";
						}
						
						if(value.length() > 2000) {
							value = value.substring(0, 1997) + "...";
						}
						ps.setString(colIndex + 1, value);
					}
					// int
					else if (type == SemossDataType.INT) {
						Integer value = null;
						String val = nextRow[colIndex].trim();
						try {
							// added to remove $ and , in data and then try
							// parsing as Double
							int mult = 1;
							if (val.startsWith("(") || val.startsWith("-"))
								mult = -1;
							val = val.replaceAll("[^0-9\\.E]", "");
							value = mult * ((Number) Double.parseDouble(val.trim())).intValue();
						} catch (NumberFormatException ex) {
							// do nothing
						}

						if (value != null) {
							ps.setInt(colIndex + 1, value);
						} else {
							// set default as null
							ps.setObject(colIndex + 1, null);
						}
					}
					// doubles
					else if (type == SemossDataType.DOUBLE) {
						Double value = null;
						String val = nextRow[colIndex].trim();
						try {
							// added to remove $ and , in data and then try
							// parsing as Double and negative number
							int mult = 1;
							if (val.startsWith("(") || val.startsWith("-"))
								mult = -1;
							val = val.replaceAll("[^0-9\\.E]", "");
							value = mult * Double.parseDouble(val.trim());
						} catch (NumberFormatException ex) {
							// do nothing
						}

						if (value != null) {
							ps.setDouble(colIndex + 1, value);
						} else {
							// set default as null
							ps.setObject(colIndex + 1, null);
						}
					} 
					// dates
					else if (type == SemossDataType.DATE) {
						// can I get a format?
						Long dTime = SemossDate.getTimeForDate(nextRow[colIndex], additionalTypes[colIndex]);
						if (dTime != null) {
							ps.setDate(colIndex + 1, new java.sql.Date(dTime));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						}
					}
					// timestamps
					else if (type == SemossDataType.TIMESTAMP) {
						// can I get a format?
						Long dTime = SemossDate.getTimeForTimestamp(nextRow[colIndex], additionalTypes[colIndex]);
						if (dTime != null) {
							ps.setTimestamp(colIndex + 1, new java.sql.Timestamp(dTime));
						} else {
							ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
						}
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					logger.info("Done inserting " + count + " number of rows");
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			logger.info("Finished");
			logger.info("Completed " + count + " number of rows");
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
			String errorMessage = "";
			if (nextRow == null) {
				errorMessage = "Error occured while performing insert on csv row number = " + count;
			} else {
				errorMessage = "Error occured while performing insert on csv data row:" + "\n" + Arrays.toString(nextRow);
			}
			throw new IOException(errorMessage);
		}
	}

	private String determineExistingTableToInsert(Map<String, Map<String, String>> existingRDBMSStructure, String[] headers, SemossDataType[] types) {
		String existingTableNameToInsert = null;

		// loop through every existing table
		TABLE_LOOP: for (String existingTableName : existingRDBMSStructure.keySet()) {
			// get the map containing the column names to data types for the existing table name
			Map<String, String> existingColTypeMap = existingRDBMSStructure.get(existingTableName);

			// if the number of headers does not match
			// we know it is not a good match
			if (existingColTypeMap.keySet().size() - 1 != headers.length) {
				// no way all columns are contained
				// look at the next table
				continue TABLE_LOOP;
			}

			// check that every header is contained in this table
			// check that the data types from the csv file and the table match
			for (int i = 0; i < headers.length; i++) {
				// existing rdbms structure returns with everything upper case
				String csvHeader = RDBMSEngineCreationHelper.cleanTableName(headers[i]).toUpperCase();
				SemossDataType csvDataType = types[i];

				if (!existingColTypeMap.containsKey(csvHeader)) {
					// if the column name doesn't exist in the existing table
					// look at the next table
					continue TABLE_LOOP;
				}

				// if we get here
				// we found a header that is in both
				SemossDataType existingColDataType = SemossDataType.convertStringToDataType(existingColTypeMap.get(csvHeader.toUpperCase()));

				// now test the data types
				// need to perform a non-exact match
				// i.e. float and double are the same, etc.
				if (csvDataType != existingColDataType) {
					// if the data types do not match
					// look at the next table
					continue TABLE_LOOP;
				}
			}

			// if we got outside the other loop
			// it means that every header was contained in the table
			// and the data types also match
			existingTableNameToInsert = existingTableName;
		}

		return existingTableNameToInsert;
	}
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		String filePath = "C:/Users/SEMOSS/Desktop/Movie Data.csv";

		Insight in = new Insight();
		PixelPlanner planner = new PixelPlanner();
		planner.setVarStore(in.getVarStore());
		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));

		RdbmsUploadTableReactor reactor = new RdbmsUploadTableReactor();
		reactor.setInsight(in);
		reactor.setPixelPlanner(planner);
		NounStore nStore = reactor.getNounStore();
		// app name struct
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata("ztest" + Utility.getRandomString(6), PixelDataType.CONST_STRING));
			nStore.addNoun(ReactorKeysEnum.APP.getKey(), struct);
		}
		// file path
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(filePath, PixelDataType.CONST_STRING));
			nStore.addNoun(ReactorKeysEnum.FILE_PATH.getKey(), struct);
		}

		reactor.In();
		reactor.execute();
	}
}
