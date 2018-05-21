package prerna.sablecc2.reactor.app.upload.rdbms.csv;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.sablecc2.reactor.app.upload.AbstractRdbmsUploadReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RdbmsFlatCsvUploadReactor extends AbstractRdbmsUploadReactor {

	/*
	 * There are quite a few things that we need
	 * 1) app -> name of the app to create
	 * 1) filePath -> string contianing the path of the file
	 * 2) delimiter -> delimiter for the file
	 * 3) dataTypes -> map of the header to the type, this will contain the original headers we send to FE
	 * 4) newHeaders -> map containign old header to new headers for the csv file
	 * 5) additionalTypes -> map containing header to an additional type specification
	 * 						additional inputs would be {header : currency, header : date_format, ... }
	 * 6) clean -> boolean if we should clean up the strings before insertion, default is true
	 * TODO: 7) deduplicate -> boolean if we should remove duplicate rows in the relational database
	 * 8) existing -> boolean if we should add to an existing app, defualt is false
	 */

	private static final String CLASS_NAME = RdbmsFlatCsvUploadReactor.class.getName();

	public RdbmsFlatCsvUploadReactor() {
		this.keysToGet = new String[]{APP, FILE_PATH, DELIMITER, DATA_TYPE_MAP, NEW_HEADERS, 
				ADDITIONAL_TYPES, CLEAN_STRING_VALUES, REMOVE_DUPLICATE_ROWS, ADD_TO_EXISTING};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		final String appName = getAppName();
		final boolean existing = getExisting();
		final String filePath = getFilePath();
		final File file = new File(filePath);
		if(!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path specified");
		}

		if(existing) {
			addToExistingApp(appName, filePath, logger);
		} else {
			generateNewApp(appName, filePath, logger);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Make a new app with the file data
	 * @param newAppName
	 * @param filePath
	 */
	@Override
	public void generateNewApp(final String newAppName, final String filePath, Logger logger) {
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

		final String delimiter = getDelimiter();
		Map<String, String> dataTypesMap = getDataTypeMap();
		Map<String, String> newHeaders = getNewHeaders();
		Map<String, String> additionalDataTypeMap = getAdditionalTypes();
		final boolean clean = getClean();

		// now that I have everything, let us go through and insert

		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(newAppName);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");

		logger.info("Starting app creation");

		logger.info("1. Start generating app folder");
		UploadUtilities.generateAppFolder(newAppName);
		logger.info("1. Complete");

		logger.info("Generate new app database");
		logger.info("2. Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppName);
		logger.info("2. Complete");

		logger.info("3. Create properties file for database...");
		File tempSmss = null;
		try {
			tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppName, owlFile, "H2_DB", null);
			DIHelper.getInstance().getCoreProp().setProperty(newAppName + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("3. Complete");

		logger.info("4. Create database store...");
		IEngine engine = new RDBMSNativeEngine();
		engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		engine.setProp(props);
		engine.openDB(null);
		logger.info("4. Complete");

		logger.info("5. Start loading data..");
		logger.info("Parsing file metadata...");
		CSVFileHelper helper = getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		Object[] headerTypesArr = getHeadersAndTypes(helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info("Done parsing file metadata");

		logger.info("Create table...");
		String tableName = RDBMSEngineCreationHelper.cleanTableName(FilenameUtils.getBaseName(filePath)).toUpperCase();
		String uniqueRowId = tableName + "_UNIQUE_ROW_ID";

		// NOTE ::: SQL_TYPES will have the added unique row id at index 0
		String[] sqlTypes = createNewTable(engine, tableName, uniqueRowId, headers, types);
		logger.info("Done create table");
		try {
			bulkInsertFile(engine, helper, tableName, headers, types, additionalTypes, clean, logger);
			addIndex(engine, tableName, uniqueRowId);
		} catch (IOException e) {
			// ugh... gotta clean up and delete everything... TODO:
			e.printStackTrace();
		}
		logger.info("5. Complete");

		logger.info("6. Start generating engine metadata...");
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), ENGINE_TYPE.RDBMS);
		generateTableMetadata(owler, tableName, uniqueRowId, headers, sqlTypes);
		owler.commit();
		try {
			owler.export();
			engine.setOWL(owlFile.getPath());
		} catch (IOException e) {
			// ugh... gotta clean up and delete everything... TODO:
			e.printStackTrace();
		}
		logger.info("6. Complete");

		logger.info("7. Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppName);
		engine.setInsightDatabase(insightDatabase);
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(engine);
		logger.info("7. Complete");

		logger.info("8. Process app metadata to allow for traversing across apps	");
		try {
			updateLocalMaster(newAppName);
			updateSolr(newAppName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("8. Complete");

		// and rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempSmss.delete();

		// update DIHelper
		updateDIHelper(newAppName, engine, smssFile);
	}

	/**
	 * Add the data into an existing rdbms engine
	 * @param appName
	 * @param filePath
	 */
	@Override
	public void addToExistingApp(String appName, String filePath, Logger logger) {
		IEngine engine = Utility.getEngine(appName);
		if(engine == null) {
			throw new IllegalArgumentException("Couldn't find the app " + appName + " to append data into");
		}
		if(!(engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}

		final String delimiter = getDelimiter();
		Map<String, String> dataTypesMap = getDataTypeMap();
		Map<String, String> newHeaders = getNewHeaders();
		Map<String, String> additionalDataTypeMap = getAdditionalTypes();
		final boolean clean = getClean();

		int stepCounter = 1;
		logger.info(stepCounter + ". Start loading data..");
		logger.info("Parsing file metadata...");
		CSVFileHelper helper = getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		Object[] headerTypesArr = getHeadersAndTypes(helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info("Done parsing file metadata");

		logger.info("Get existing database schema...");
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		logger.info("Done getting existing database schema");

		logger.info("Determine if we can add into an existing table or make new table...");
		String tableToInsertInto = determineExistingTableToInsert(existingRDBMSStructure, headers, types);
		if(tableToInsertInto == null) {
			logger.info("Could not find existing table to insert into");
			logger.info("Create table...");
			tableToInsertInto = RDBMSEngineCreationHelper.cleanTableName(FilenameUtils.getBaseName(filePath).replace(" ", "_")).toUpperCase();
			String uniqueRowId = tableToInsertInto + "_UNIQUE_ROW_ID";

			// NOTE ::: SQL_TYPES will have the added unique row id at index 0
			String[] sqlTypes = createNewTable(engine, tableToInsertInto, uniqueRowId, headers, types);
			logger.info("Done create table");
			try {
				bulkInsertFile(engine, helper, tableToInsertInto, headers, types, additionalTypes, clean, logger);
				addIndex(engine, tableToInsertInto, uniqueRowId);
			} catch (IOException e) {
				// ugh... gotta clean up and delete everything... TODO:
				e.printStackTrace();
			}
			logger.info(stepCounter + ". Complete");
			stepCounter++;

			logger.info(stepCounter + ". Start generating engine metadata...");
			OWLER owler = new OWLER(engine, engine.getOWL());
			generateTableMetadata(owler, tableToInsertInto, uniqueRowId, headers, sqlTypes);
			owler.commit();
			try {
				owler.export();
				engine.setOWL(engine.getOWL());
			} catch (IOException e) {
				// ugh... gotta clean up and delete everything... TODO:
				e.printStackTrace();
			}
			logger.info(stepCounter + ". Complete");

			logger.info(stepCounter + ". Start generating default app insights");
			Set<String> newTableSet = new HashSet<String>();
			newTableSet.add(tableToInsertInto);
			RDBMSEngineCreationHelper.insertNewTablesAsInsights(engine, newTableSet);
			logger.info(stepCounter + ". Complete");
			stepCounter++;
		} else {
			logger.info("Found table " + tableToInsertInto + " that holds similar data! Will insert into this table");
			try {
				bulkInsertFile(engine, helper, tableToInsertInto, headers, types, additionalTypes, clean, logger);
			} catch (IOException e) {
				// ugh... gotta clean up and delete everything... TODO:
				e.printStackTrace();
			}
			logger.info(stepCounter + ". Complete");
		}

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		try {
			updateLocalMaster(appName);
			updateSolr(appName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info(stepCounter + ". Complete");
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
			boolean clean, Logger logger) throws IOException {

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
			while( (nextRow = helper.getNextRow()) != null ) {
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					// nulls get added as null
					// not interesting...
					if(nextRow[colIndex] == null) {
						ps.setObject(colIndex+1, null);
						continue;
					}

					// yay, actual data
					SemossDataType type = types[colIndex];
					// strings
					if(type == SemossDataType.STRING || type == SemossDataType.FACTOR) {
						if(clean) {
							String value = Utility.cleanString(nextRow[colIndex], false);
							ps.setString(colIndex+1, value + "");
						} else {
							ps.setString(colIndex+1, nextRow[colIndex] + "");
						}
					} 
					// int
					else if(type == SemossDataType.INT) {
						Integer value = null;
						String val = nextRow[colIndex].trim();
						try {
							//added to remove $ and , in data and then try parsing as Double
							int mult = 1;
							if(val.startsWith("(") || val.startsWith("-")) // this is a negativenumber
								mult = -1;
							val = val.replaceAll("[^0-9\\.E]", "");
							value = mult * Integer.parseInt(val.trim());
						} catch(NumberFormatException ex) {
							//do nothing
						}

						if(value != null) {
							ps.setInt(colIndex+1, value);
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					}
					// doubles
					else if(type == SemossDataType.DOUBLE) {
						Double value = null;
						String val = nextRow[colIndex].trim();
						try {
							//added to remove $ and , in data and then try parsing as Double
							int mult = 1;
							if(val.startsWith("(") || val.startsWith("-")) // this is a negativenumber
								mult = -1;
							val = val.replaceAll("[^0-9\\.E]", "");
							value = mult * Double.parseDouble(val.trim());
						} catch(NumberFormatException ex) {
							//do nothing
						}

						if(value != null) {
							ps.setDouble(colIndex+1, value);
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					} 
					// dates
					else if(type == SemossDataType.DATE) {
						// can I get a format?
						String format = additionalTypes[colIndex];
						if(format != null && !format.isEmpty()) {
							java.util.Date value = Utility.getDateObjFromStringFormat(nextRow[colIndex], format);
							if(value != null) {
								ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
							} else {
								// set default as null
								ps.setObject(colIndex+1, null);
							}
						} else {
							java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex]);
							if(value != null) {
								ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
							} else {
								// set default as null
								ps.setObject(colIndex+1, null);
							}
						}
					}
					// timestamps
					else if(type == SemossDataType.TIMESTAMP) {
						// can I get a format?
						String format = additionalTypes[colIndex];
						if(format != null && !format.isEmpty()) {
							java.util.Date value = Utility.getDateObjFromStringFormat(nextRow[colIndex], format);
							if(value != null) {
								ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
							} else {
								// set default as null
								ps.setObject(colIndex+1, null);
							}
						} else {
							java.util.Date value = Utility.getTimeStampAsDateObj(nextRow[colIndex]);
							if(value != null) {
								ps.setTimestamp(colIndex+1, new java.sql.Timestamp(value.getTime()));
							} else {
								// set default as null
								ps.setObject(colIndex+1, null);
							}
						}
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if(++count % batchSize == 0) {
					logger.info("Done inserting " + count + " number of rows");
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			logger.info("Finished");
			logger.info("Completed " + count + " number of rows");
			ps.close();
		} catch(SQLException e) {
			e.printStackTrace();
			String errorMessage = "";
			if(nextRow == null) {
				errorMessage = "Error occured while performing insert on csv on row number = " + count;
			} else { 
				errorMessage = "Error occured while performing insert on csv data row:"
						+ "\n" + Arrays.toString(nextRow);
			}
			throw new IOException(errorMessage);
		}
	}

	/**
	 * Parse the file
	 * @param filePath
	 * @param delimiter
	 * @param dataTypesMap
	 * @param newHeaders
	 * @return
	 */
	private CSVFileHelper getHelper(final String filePath, final String delimiter, 
			Map<String, String> dataTypesMap, Map<String, String> newHeaders) {
		CSVFileHelper csvHelper = new CSVFileHelper();
		csvHelper.setDelimiter(delimiter.charAt(0));
		csvHelper.parse(filePath);

		// specify the columns to use
		// default will include all
		if(dataTypesMap != null && dataTypesMap.isEmpty()) {
			Set<String> headersToUse = new TreeSet<String>(dataTypesMap.keySet());
			csvHelper.parseColumns(headersToUse.toArray(new String[]{}));
		}

		// if the user has cleaned any headers
		if(newHeaders != null && !newHeaders.isEmpty()) {
			csvHelper.modifyCleanedHeaders(newHeaders);
		}

		return csvHelper;
	}

	/**
	 * Figure out the types and how to use them
	 * Will return an object[]
	 * Index 0 of the return is an array of the headers
	 * Index 1 of the return is an array of the types
	 * Index 2 of the return is an array of the additional type information
	 * The 3 arrays all match based on index
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 */
	private Object[] getHeadersAndTypes(CSVFileHelper helper, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap) {
		String[] headers = helper.getHeaders();
		int numHeaders = headers.length;
		// we want types
		// and we want additional types
		SemossDataType[] types = new SemossDataType[numHeaders];
		String[] additionalTypes = new String[numHeaders];

		// get the types
		if(dataTypesMap != null && !dataTypesMap.isEmpty()) {
			for(int i = 0; i < numHeaders; i++) {
				types[i] = SemossDataType.convertStringToDataType(dataTypesMap.get(headers[i]));
			}
		} else {
			String[] predictedTypes = helper.predictTypes();
			for(int i = 0; i < predictedTypes.length; i++) {
				types[i] = SemossDataType.convertStringToDataType(predictedTypes[i]);
			}
		}

		// get additional type information
		if(additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
			for(int i = 0 ; i < numHeaders; i++) {
				additionalTypes[i] = additionalDataTypeMap.get(headers[i]);
			}
		}

		return new Object[]{headers, types, additionalTypes};
	}

	private String determineExistingTableToInsert(Map<String, Map<String, String>> existingRDBMSStructure, String[] headers, SemossDataType[] types) {
		String existingTableNameToInsert = null;

		// loop through every existing table
		TABLE_LOOP : for(String existingTableName : existingRDBMSStructure.keySet()) {
			// get the map containing the column names to data types for the existing table name
			Map<String, String> existingColTypeMap = existingRDBMSStructure.get(existingTableName);

			// if the number of headers does not match
			// we know it is not a good match
			if(existingColTypeMap.keySet().size()-1 != headers.length) {
				// no way all columns are contained
				// look at the next table
				continue TABLE_LOOP;
			}

			// check that every header is contained in this table
			// check that the data types from the csv file and the table match
			for(int i = 0; i < headers.length; i++) {
				// existing rdbms structure returns with everything upper case
				String csvHeader = RDBMSEngineCreationHelper.cleanTableName(headers[i]).toUpperCase();
				SemossDataType csvDataType = types[i];

				if(!existingColTypeMap.containsKey(csvHeader)) {
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
				if(csvDataType != existingColDataType) {
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

	///////////////////////////////////////////////////////

	/*
	 * Getters from noun store
	 */

	private Map<String, String> getDataTypeMap() {
		GenRowStruct grs = this.store.getNoun(DATA_TYPE_MAP);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	private Map<String, String> getNewHeaders() {
		GenRowStruct grs = this.store.getNoun(NEW_HEADERS);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	private Map<String, String> getAdditionalTypes() {
		GenRowStruct grs = this.store.getNoun(ADDITIONAL_TYPES);
		if(grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		String filePath = "C:/Users/SEMOSS/Desktop/Movie Data.csv";

		Insight in = new Insight();
		PixelPlanner planner = new PixelPlanner();
		planner.setVarStore(in.getVarStore());
		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));

		RdbmsFlatCsvUploadReactor reactor = new RdbmsFlatCsvUploadReactor();
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
