package prerna.poi.main;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

@Deprecated
public class RDBMSFlatCSVUploader extends AbstractCSVFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSFlatCSVUploader.class.getName());
	
	// hold the existing rdbms structure
	private Map<String, Map<String, String>> existingRDBMSStructure;
	
	// this will hold the headers to the data types for each csv file
	private List<Map<String, String[]>> dataTypeMapList;
	private Map<String, String[]> dataTypeMap;
	
	// need to keep track of the new tables added when doing addToExisting
	private Map<String, String> newTables = new Hashtable<String, String>();
	
	// these keys are used within the return of the parseCSVData to get the
	// headers and data types from a given csv file
	public static final String CSV_HEADERS = "headers";
	public static final String CSV_DATA_TYPES = "dataTypes";
	
	// used as a default for the unique row id
	private final String BASE_PRIM_KEY = "_UNIQUE_ROW_ID";
	
	// cases when we do not want to perform any cleaning on the values
	private boolean cleanString = true;
	
	///////////////////////////////////////// main upload methods //////////////////////////////////////////
	
	/**
	 * Load a new flat table rdbms database
	 * @param smssLocation						The location of the smss file
	 * @param fileLocations						The location of the files
	 * @param customBaseURI						customBaseURI to set... need to remove this stupid thing
	 * @param owlPath							The path to the owl file
	 * @param dbName							The name of the database driver
	 * @param dbDriverType						The database type (h2, mysql, etc.)
	 * @param allowDuplicates					Boolean to determine if we should delete duplicate rows
	 * @return									The new engine created
	 * @throws IOException 
	 */
	public IEngine importFileWithOutConnection(ImportOptions options) throws IOException {
		String smssLocation = options.getSMSSLocation();
		String engineName = options.getDbName();
		String appID = options.getEngineID();
		String fileLocations = options.getFileLocations();
		String customBaseURI = options.getBaseUrl();
		String owlPath = options.getOwlFileLocation();	
		cleanString = options.getCleanString();
		
		RdbmsTypeEnum dbDriverType = options.getRDBMSDriverType();
		queryUtil = SQLQueryUtil.initialize(dbDriverType);
		
		boolean error = false;
		// sets the custom base uri, sets the owl path, sets the smss location
		// and returns the fileLocations split into an array based on ';' character
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);
		LOGGER.setLevel(Level.WARN);
		try {
			// create the engine and the owler
			openRdbmsEngineWithoutConnection(engineName, appID);
			Map<String, String> paramHash = new Hashtable<String, String>();
			paramHash.put("BaseFolder", DIHelper.getInstance().getProperty(Constants.BASE_FOLDER));
			paramHash.put("ENGINE", engineName);

			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				fileName = Utility.fillParam2(fileName, paramHash);
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				if(i == 0) {
					existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
				} else {
					// need to update to get the rdbms structure to determine how the new files should be added
					existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
				}
				// similar to other csv reading
				// we load the user defined types
				if(dataTypeMapList != null && !dataTypeMapList.isEmpty()) {
					dataTypeMap = dataTypeMapList.get(i); 
				}
				// note that the csvHelper gets created in processTable
				processTable(fileName, dataTypeMap);
			}
			// add indexes for faster searching
			addIndexes();
			// write the owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, this.owler);
		} catch(IOException e) {
			e.printStackTrace();
			error = true;
			String errorMessage = e.getMessage();
			if(errorMessage == null || errorMessage.trim().isEmpty()) {
				errorMessage = "Uknown error occured...";
			}
			throw new IOException(errorMessage);
		} catch(Exception e) {
			e.printStackTrace();
			error = true;
			String errorMessage = e.getMessage();
			if(errorMessage == null || errorMessage.trim().isEmpty()) {
				errorMessage = "Uknown error occured...";
			}
			throw new IOException(errorMessage);
		} finally {
			// close the helper
			csvHelper.clear();
			// close other stuff
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
		}

		return engine;
	}
	
	/**
	 * Load a new flat table into an existing rdbms engine
	 * @param smssLocation						The location of the smss file
	 * @param fileLocations						The location of the files
	 * @param customBaseURI						customBaseURI to set... need to remove this stupid thing
	 * @param owlPath							The path to the owl file
	 * @param dbDriverType						The database type (h2, mysql, etc.)
	 * @param allowDuplicates					Boolean to determine if we should delete duplicate rows
	 * @throws IOException 
	 */
	public void importFileWithConnection(ImportOptions options) throws IOException {
		String smssLocation = options.getSMSSLocation();
		String engineName = options.getDbName();
		String fileLocations = options.getFileLocations();
		String customBaseURI = options.getBaseUrl();
		String owlPath = options.getOwlFileLocation();		
		autoLoad = options.isAutoLoad();
		cleanString = options.getCleanString();
		
		RdbmsTypeEnum dbDriverType = options.getRDBMSDriverType();
		queryUtil = SQLQueryUtil.initialize(dbDriverType);
		
		Hashtable <String, String> paramHash2 = new Hashtable<String, String>();
		paramHash2.put("engine", engineName);
		owlPath = Utility.fillParam2(owlPath, paramHash2);
		
		boolean error = false;
		// sets the custom base uri, sets the owl path, sets the smss location
		// and returns the fileLocations split into an array based on ';' character
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);

		LOGGER.setLevel(Level.WARN);
		try {
			// create the engine and the owler
			openEngineWithConnection(engineName);
			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				// need to update to get the rdbms structure to determine how the new files should be added
				existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
				
				// similar to other csv reading
				// we load the user defined types
				if(dataTypeMapList != null && !dataTypeMapList.isEmpty()) {
					dataTypeMap = dataTypeMapList.get(i);
				}
				// note that the csvHelper gets created in processTable
				// this call is not needed
				// I do not know if the file was changed.. so I am going to let it generate it at this point
				// I will change it later
				processTable(fileName, dataTypeMap);
			}
			// add indexes for faster searching
			addIndexes();
			// write the owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.insertNewTablesAsInsights(this.engine, this.owler, newTables.keySet());
		} catch(IOException e) {
			e.printStackTrace();
			error = true;
			String errorMessage = e.getMessage();
			if(errorMessage == null || errorMessage.trim().isEmpty()) {
				errorMessage = "Uknown error occured...";
			}
			throw new IOException(errorMessage);
		} finally {
			// close the helper
			csvHelper.clear();
			// close other stuff
			if(error || autoLoad) {
				closeDB();
				closeOWL();
			} else {
				commitDB();
			}
		}
	}
	
	
	///////////////////////////////////////// end main upload methods //////////////////////////////////////////

	
	/**
	 * This is used to add additional flat tables into the engine
	 * Method will determine if we need a new table vs. need to add data into an existing engine
	 * 
	 * TODO: need to figure out how to take in join information to add into the OWL!!!!
	 * TODO: need to figure out how to take in join information to add into the OWL!!!!
	 * TODO: need to figure out how to take in join information to add into the OWL!!!!
	 * TODO: need to figure out how to take in join information to add into the OWL!!!!
	 * TODO: need to figure out how to take in join information to add into the OWL!!!!
	 * @param dataTypeMap 
	 * 
	 * 
	 * @param fileLocation					The location of the csv file
	 * @throws SQLException 
	 */
	private void processTable(final String FILE_LOCATION, Map<String, String[]> csvMeta) throws IOException {
		
		// if csvMeta is null, we are not getting data type information from the FE, and we need to create this
		// using all the file info
		if(csvMeta == null || csvMeta.isEmpty()) {
			// parse the csv meta to get the headers and data types
			// headers and data types arrays match based on position 
			// currently assume we are loading all the columns
			// currently not taking in the dataTypes
			csvMeta = parseCSVData(FILE_LOCATION);
		} else {
			parseCSV(FILE_LOCATION, csvMeta);
		}
		
		/*
		 * We need to determine if we are going to create a new table or append onto an existing one
		 * Requirements for inserting into an existing table:
		 * 
		 * 1) If all the headers are contained in an existing single table,
		 * then we go ahead and insert into that table and keep everything else empty where columns
		 * might be missing
		 * 
		 * 2) The data types are the same within the csv file and the existing column in the table
		 */

		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		
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
				String csvDataType = dataTypes[i].toUpperCase();
				
				if(!existingColTypeMap.containsKey(csvHeader)) {
					// if the column name doesn't exist in the existing table
					// look at the next table
					continue TABLE_LOOP;
				}
				
				// if we get here
				// we found a header that is in both
				String existingColDataType = existingColTypeMap.get(csvHeader.toUpperCase());
				
				// now test the data types
				// need to perform a non-exact match
				// i.e. float and double are the same, etc.
				if(!equivalentDataTypes(existingColDataType, csvDataType)) {
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
		
		// if it isn't null, we found a table to insert into, lets do it
		if(existingTableNameToInsert != null) {
			// just add into the table we found that matches
			insertCSVIntoExistingTable(FILE_LOCATION, existingTableNameToInsert, csvMeta);
			
		} else {
			// didn't find a table to insert into
			// create a new table
			
			// based on normal upload flow, before this point is reached, the FileUploader appends 
			// the upload time as "_yyyy_MM_dd_HH_mm_ss_SSSS" onto the original fileName in 
			// order to ensure that it is a unique file name
			// let us try and remove this
			String fileName = Utility.getOriginalFileName(FILE_LOCATION);
			// make the table name based on the fileName
			String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
			
			// we also need to make sure the table is unique
			int counter = 2;
			String uniqueTableName = cleanTableName;
			while(existingRDBMSStructure.containsKey(uniqueTableName)) {
				// TODO: might want a better way to do this
				// 		but i think this is pretty unlikely... maybe...
				uniqueTableName = cleanTableName + "_" + counter;
				counter++;
			}
			
			// run the process to create a new table from the csv file
			generateNewTableFromCSV(FILE_LOCATION, uniqueTableName, csvMeta);
			
			//TODO: this is where join information from the user would need to be added
			// if we want the new table to be able to be joined onto existing tables
			
			// default behavior if none found, join on column names that are the same
			joinTables(cleanTableName, csvMeta);
		}
	}
	
	///////////////////////////////// utility methods ///////////////////////////////
	
	/**
	 * Parse the CSV to get the header and data type for each column
	 * @param fileLocation				The location of the csv file
	 * @return							Map containing the headers and data types, aligned by position
	 * 									Example of a return map:
	 * 									{
	 * 										"headers" -> [Title, Genre, Movie_Budget],
	 * 										"dataTypes" -> [VARCHAR(800), VARCHAR(800), FLOAT];
	 * 									}
	 * 									Index i of headers array matches to index i for dataTypes array
	 * 									for i = 0 to length of headers array
	 */
	private Map<String, String[]> parseCSVData(final String FILE_LOCATION) {
		LOGGER.info("Processing csv file: " + FILE_LOCATION);

		// use the csv file helper to load the data
		csvHelper = new CSVFileHelper();
		// assume csv
		csvHelper.setDelimiter(',');
		csvHelper.parse(FILE_LOCATION);
		
		// modify the clean header names
		if(userHeaderNames != null) {
			Map<String, String> thisFileHeaderChanges = userHeaderNames.get(FILE_LOCATION);
			if(thisFileHeaderChanges != null && !thisFileHeaderChanges.isEmpty()) {
				csvHelper.modifyCleanedHeaders(thisFileHeaderChanges);
			}
		}
		String[] headers = csvHelper.getHeaders();
		LOGGER.info("Found headers: " + Arrays.toString(headers));
		
		Object[][] typePredictions = csvHelper.predictTypes();
		String[] dataTypes = new String[typePredictions.length];
		for(int i = 0; i < typePredictions.length; i++) {
			dataTypes[i] = typePredictions[i][1].toString();
		}
		LOGGER.info("Found data types: " + Arrays.toString(dataTypes));

		Map<String, String[]> csvMeta = new Hashtable<String, String[]>();
		csvMeta.put(CSV_HEADERS, headers);
		csvMeta.put(CSV_DATA_TYPES, dataTypes);
		
		return csvMeta;
	}
	
	private void parseCSV(final String FILE_LOCATION, Map<String, String[]> csvMeta) {
		LOGGER.info("Processing csv file: " + FILE_LOCATION);

		// use the csv file helper to load the data
		csvHelper = new CSVFileHelper();
		// assume csv
		csvHelper.setDelimiter(',');
		csvHelper.parse(FILE_LOCATION);
		
		// modify the clean header names
		if(userHeaderNames != null) {
			Map<String, String> thisFileHeaderChanges = userHeaderNames.get(FILE_LOCATION);
			if(thisFileHeaderChanges != null && !thisFileHeaderChanges.isEmpty()) {
				csvHelper.modifyCleanedHeaders(thisFileHeaderChanges);
			}
		}
		// set the columns to get
		String[] headersToUse = csvMeta.get(CSV_HEADERS);
		csvHelper.parseColumns(headersToUse);
		
		// we need to convert from the generic data types from the FE to the sql specific types
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		if(sqlHash.isEmpty()) {
			createSQLTypes();
		}
		int numCols = headersToUse.length;
		String[] sqlDataTypes = new String[numCols];
		for(int colIdx = 0; colIdx < numCols; colIdx++) {
			if(sqlHash.containsKey(dataTypes[colIdx])) {
				sqlDataTypes[colIdx] = sqlHash.get(dataTypes[colIdx]);
			} else {
				sqlDataTypes[colIdx] = dataTypes[colIdx];
			}
		}
		
		// now update the meta data
		csvMeta.put(CSV_DATA_TYPES, sqlDataTypes);
	}
	
	/**
	 * Create a new table using the csv file
	 * Add an additional identity column to the table to be the primary key (defined through the OWL, not through the database)
	 * @param FILE_LOCATION						The location of the csv file
	 * @param TABLE_NAME						The name of the table to create
	 * @param csvMeta							Map containing the header and data type for each column, aligned by position
	 * @throws SQLException 
	 * @throws IOException 
	 */
	private void generateNewTableFromCSV(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) throws IOException {
		LOGGER.info("Creating a new table from " + FILE_LOCATION);

		// in the case of H2 RDBMS (and i'm guessing this is the case for other RDBMS)
		// dates are required to be sent in a specific format
		// thus, we want to look at the data types and see if there is a date
		// if there is a date, we need to perform a bulk insert, where we convert every date object to the correct format
		
		// define the identity column
		final String UNIQUE_ROW_ID = TABLE_NAME + BASE_PRIM_KEY;

//		if(containsDateDataType(csvMeta.get(CSV_DATA_TYPES)) || !allHeadersUsed(csvMeta.get(CSV_HEADERS))) {
			// we had a date!
			// first create the table
			// and define the identity column for it as well
			createTable(TABLE_NAME, csvMeta, UNIQUE_ROW_ID);
			// this logic will be to do a bulk insert
			bulkInsertCSVFile(FILE_LOCATION, TABLE_NAME, csvMeta);
//		} 
		
//		else {
//			// this logic just grabs the csv file and creates the table using it all in one go
//			generateCreateTableFromCSVSQL(FILE_LOCATION, TABLE_NAME, csvMeta);
//		}
		
		// now, we are doing this on table creation
		// now we need to append an identity column for the table, this will be the prim key
		// add the unique id for the table
//		addIdentityColumnToTable(TABLE_NAME, UNIQUE_ROW_ID);
		
		// now need to add the table onto the owl file
		addTableToOWL(TABLE_NAME, UNIQUE_ROW_ID, csvMeta);
	}

	/**
	 * Inserts the csv data into an existing table
	 * @param FILE_LOCATION						The location of the csv file
	 * @param TABLE_NAME						The name of the table to insert the csv data into
	 * @param csvMeta							Map containing the header and data type for each column, aligned by position
	 * @throws SQLException 
	 */
	private void insertCSVIntoExistingTable(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) throws IOException {
		/*
		 * TODO: need to determine if creating a temp table is better than always inserting 
		 * the records directly into the table... this is currently done under the assumption
		 * that the CSV bulk insert is better than the bulk insert i am doing since i need to
		 * cast the datat types
		 * NOTE: we can only do the load via SQL if there are no dates.  if we have dates,
		 * we need to do our own insert since we must convert date objects
		 * 
		 * If no dates are found
		 * 		This will be done in 3 steps
		 * 		1) load csv file into a temp table
		 * 		2) insert the temp table into the existing table
		 *		3) drop the temp table
		 *
		 * If there are dates
		 * 		We will just iterate through the csv file and insert
		 */
		
//		if(containsDateDataType(csvMeta.get(CSV_DATA_TYPES)) || !allHeadersUsed(csvMeta.get(CSV_HEADERS))) {
			// perform a bulk insert into the table
			bulkInsertCSVFile(FILE_LOCATION, TABLE_NAME, csvMeta);
//		} else {
//			// no date found... lets do steps 1-3
//			final String TEMP_TABLE = "TEMP_TABLE_98712396874";
//			// 1) create the temp table
//			generateCreateTableFromCSVSQL(FILE_LOCATION, TEMP_TABLE, csvMeta);
//			// 2) create query to insert temp into existing table
//			insertTableIntoOtherTable(TABLE_NAME, TEMP_TABLE, csvMeta);
//			// 3) drop current table
//			this.engine.removeData("DROP TABLE " + TEMP_TABLE);
//		}
	}
	
	/**
	 * Generate the query to create a new table with the csv data
	 * @param FILE_LOCATION				The location of the csv file
	 * @param TABLE_NAME				The name of the table to load the csv file into
	 * @param csvMeta					The column names and types of the csv file
	 * @return							The query to load the csv file into the table
	 */
	private void generateCreateTableFromCSVSQL(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		
		// generate create table statement
		StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" (");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			queryBuilder.append(cleanHeader.toUpperCase());
			queryBuilder.append(" ");
			queryBuilder.append(dataTypes[headerIndex].toUpperCase());
			
			// add a comma if NOT the last index
			if(headerIndex != headers.length-1) {
				queryBuilder.append(", ");
			}
		}
		queryBuilder.append(") AS SELECT * FROM CSVREAD('");
		queryBuilder.append(FILE_LOCATION);
		queryBuilder.append("')");
		
		// load the csv as a table
		System.out.println(queryBuilder.toString());
		try {
			this.engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates a table using the table name and columns/types specified in csvMeta
	 * @param TABLE_NAME				The name of the table
	 * @param csvMeta					Map containing the headers and column types
	 * 									for the table
	 * @param UNIQUE_ROW_ID 
	 */
	private void createTable(final String TABLE_NAME, Map<String, String[]> csvMeta, final String UNIQUE_ROW_ID) {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		
		// need to first create the table
		StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" (").append(UNIQUE_ROW_ID).append(" IDENTITY, ");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			queryBuilder.append(cleanHeader.toUpperCase());
			queryBuilder.append(" ");
			queryBuilder.append(dataTypes[headerIndex].toUpperCase());
			
			// add a comma if NOT the last index
			if(headerIndex != headers.length-1) {
				queryBuilder.append(", ");
			}
		}
		queryBuilder.append(")");
		LOGGER.info("CREATE TABLE QUERY : " + queryBuilder.toString());
		try {
			this.engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		newTables.put(TABLE_NAME, UNIQUE_ROW_ID);
	}
	
	
	/**
	 * Generates a table and does a bulk insert of all the data from the csv file
	 * @param FILE_LOCATION					
	 * @param TABLE_NAME
	 * @param csvMeta
	 * @throws SQLException 
	 */
	private void bulkInsertCSVFile(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) throws IOException {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		
		// now we need to loop through the csv data and cast to the appropriate type and insert
		// let us be smart about this and use a PreparedStatement for bulk insert
		// get the bulk statement
		
		// the prepared statement requires the table name and then the list of columns
		Object[] getPreparedStatementArgs = new Object[headers.length+1];
		getPreparedStatementArgs[0] = TABLE_NAME;
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			getPreparedStatementArgs[headerIndex+1] = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
		}
		PreparedStatement ps = (PreparedStatement) this.engine.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);
		
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
		
		// we loop through every row of the csv
		String[] nextRow = null;
		try {
			while( (nextRow  = this.csvHelper.getNextRow()) != null ) {
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = dataTypes[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex]);
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					} else if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT") || type.equalsIgnoreCase("LONG")) {
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
					} else {
						if(cleanString) {
							String value = Utility.cleanString(nextRow[colIndex], false);
							ps.setString(colIndex+1, value + "");
						} else {
							ps.setString(colIndex+1, nextRow[colIndex] + "");
						}
					}
				}
				// add it
				ps.addBatch();
				
				// batch commit based on size
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			
			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
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
	 * Generate the query to create a new table with the csv data
	 * @param FILE_LOCATION				The location of the csv file
	 * @param TABLE_NAME				The name of the table to load the csv file into
	 * @param csvMeta					The column names and types of the csv file
	 */
	private void insertTableIntoOtherTable(final String BASE_TABLE, final String TABLE_TO_INSERT, Map<String, String[]> csvMeta) {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		
		// generate create table statement
		StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
		queryBuilder.append(BASE_TABLE);
		queryBuilder.append(" (");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			queryBuilder.append(cleanHeader.toUpperCase());
			
			// add a comma if NOT the last index
			if(headerIndex != headers.length-1) {
				queryBuilder.append(", ");
			}
		}
		queryBuilder.append(") SELECT * FROM ");
		queryBuilder.append(TABLE_TO_INSERT);
		
		System.out.println(queryBuilder.toString());
		try {
			this.engine.insertData(queryBuilder.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Generate the query to append an identity column onto the table
	 * @param TABLE_NAME				The name of the table
	 * @param IDENTITY_COL_NAME			The name of the identity column
	 */
//	private void addIdentityColumnToTable(final String TABLE_NAME, final String IDENTITY_COL_NAME) {
//		StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
//		queryBuilder.append(TABLE_NAME);
//		queryBuilder.append(" ADD ").append(IDENTITY_COL_NAME).append(" IDENTITY");
//		
//		System.out.println(queryBuilder.toString());
//		this.engine.insertData(queryBuilder.toString());
//	}
	
	/**
	 * Adds the metadata of a new table onto the OWL.  The identity column name becomes the primary key for the table
	 * as defined by the OWL
	 * 
	 * This adds the following triples:
	 * Assume semoss: = http://semoss.org/ontologies
	 * Assume table name = T
	 * Assume identity column name = U
	 * 
	 * 1) { <semoss:Concept/U/T> <rdfs:subClassOf> <semoss:Concept> }
	 * 2) { <semoss:Concept/U/T> <rdfs:Class> 'TYPE:LONG' }
	 * 3) { <semoss:Concept/U/T> <semoss:Relation/Conceptual> <semoss:Concept/U> }
	 * 
	 * 
	 * @param TABLE_NAME					The name of the table
	 * @param IDENTITY_COL_NAME				The name of the identity column, the prim key of the table
	 * @param csvMeta						The column names and types of the csv file
	 */
	private void addTableToOWL(final String TABLE_NAME, final String IDENTITY_COL_NAME, Map<String, String[]> csvMeta) {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
				
		// need to add metadata
		// the unique row id becomes the primary key for every other 
		// column in the csv file
		// TODO: should we add it as LONG, or as VARCHAR... 
		//		as a VARCHAR, it would make it default to always (and only) be used with counts via the UI
		//		don't see why a person would every want to do sum/avg/etc. on it....
		owler.addConcept(TABLE_NAME, IDENTITY_COL_NAME, "LONG");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			owler.addProp(TABLE_NAME, IDENTITY_COL_NAME, cleanHeader, dataTypes[headerIndex]);
		}
	}
	
	/**
	 * Looks through all the existing column names and tries to join the new table with the existing tables
	 * that are present. Join is only defined on the OWL itself, not the data
	 * 
	 * This adds the following triples:
	 * TODO: fill in
	 * 
	 * 
	 * 
	 * 
	 * @param TABLE_NAME					The name of the table
	 * @param csvMeta						The column names and types of the csv file
	 */
	private void joinTables(final String TABLE_NAME, Map<String, String[]> csvMeta) {
//		String[] headers = csvMeta.get(CSV_HEADERS);
//
//		// loop through every existing table
//		for(String existingTableName : existingRDBMSStructure.keySet()) {
//			// get the map containing the column names to data types for the existing table name
//			Map<String, String> existingColTypeMap = existingRDBMSStructure.get(existingTableName);
//			
//			// check if a header in that table matches a header in the new table added
//			for(int i = 0; i < headers.length; i++) {
//				// existing rdbms structure returns with everything upper case
//				String csvHeader = RDBMSEngineCreationHelper.cleanTableName(headers[i]).toUpperCase();
//				if(existingColTypeMap.containsKey(csvHeader)) {
//					// we have a match!
//					// lets add a relationship between them
//					// realize the concepts are joined based on a property, not based on the prim keys
//					// thus, we can't use the default predicate that owler would create
//					String predicate = existingTableName + "." + csvHeader + "." + TABLE_NAME + "." + csvHeader;
//					
//					// assumption that the column is the name of the table with the base_prim_key constant for both tables
//					owler.addRelation(existingTableName, existingTableName + BASE_PRIM_KEY, TABLE_NAME, TABLE_NAME + BASE_PRIM_KEY, predicate);
//					
//					// TODO: assumption that the first one to match is it
//					// if we add the others, each time we join I will have no idea which one is selected...
//					break;
//				}
//			}
//		}
	}
	
	/**
	 * Determine if the data types are equivalent
	 * Does a non string match for types - i.e. float and double are the same, etc.
	 * @param existingDataType
	 * @return
	 */
	private boolean equivalentDataTypes(String existingDataType, String csvDataType) {
		existingDataType = existingDataType.toUpperCase();
		csvDataType = csvDataType.toUpperCase();
		
		// if both are equal.. done deal
		if(existingDataType.equals(csvDataType)) {
			return true;
		}
		
		// a long list of things i am considering the same
		// if both are some type of varchar
		if(existingDataType.contains("VARCHAR") && csvDataType.contains("VARCHAR")) {
			return true;
		}
		// if both are some kind of double
		if(existingDataType.contains("DOUBLE") && csvDataType.contains("DOUBLE")) {
			return true;
		}
		// if both are some kind of date
		if(existingDataType.contains("DATE") && csvDataType.contains("DATE")) {
			return true;
		}
		
		// if one is a float and the other a double
		if( (existingDataType.contains("FLOAT") && csvDataType.contains("DOUBLE") ) || 
				( existingDataType.contains("DOUBLE") && csvDataType.contains("FLOAT") ) ) {
			return true;
		}
		
		//TODO: need to expand this to include other things.... too lazy to do this right now
		
		return false;
	}
	
	public void setCleanString(boolean cleanString) {
		this.cleanString = cleanString;
	}
	
	private void addIndexes() {
		for(String tableName : newTables.keySet()) {
			addColumnIndex(tableName, newTables.get(tableName));
		}
	}
	
	/**
	 * Add an index into the table for faster searching
	 * @param tableName
	 * @param colName
	 */
	protected void addColumnIndex(String tableName, String colName) {
		String indexName = colName + "_INDEX" ;
		String indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + colName + ")";
		try {
			engine.insertData(indexSql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Utility method to see if there is a date type
	 * Dates have to be in a specific format, so we need to perform bulk insert
	 * when this is true
	 * @param dataTypes				String[] containing all the data types for each column
	 * @return						boolean if one of the data types is a date
	 */
	private boolean containsDateDataType(String[] dataTypes) {
		for(String dataType : dataTypes) {
			if(dataType.equalsIgnoreCase("DATE")) {
				return true;
			}
		}
		return false;
	}
	
	private boolean allHeadersUsed(String[] headersToUse) {
		String[] csvHeaders = csvHelper.getAllCSVHeaders();
		if(csvHeaders.length == headersToUse.length) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Set the data types and headers to load for each csv file
	 * @param dataTypeMap
	 */
	public void setDataTypeMapList(List<Map<String, String[]>> dataTypeMapList) {
		this.dataTypeMapList = dataTypeMapList;
	}
	
	public Set<String> getNewTables() {
		return this.newTables.keySet();
	}
	
	//////////////////////////////// end utility methods //////////////////////////////
	

	///////////////////////////////// test methods /////////////////////////////////
	
	public static void main(String[] args) throws IOException, SQLException {
		
		long start = System.currentTimeMillis();
		
		// run this test when the server is not running
		// this will create the db and smss file so when you start the server
		// the database created will be picked up and exposed
		TestUtilityMethods.loadDIHelper();

		// set the file
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\pregnancy.csv";
		
		// set a bunch of db stuff
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String engineName = "abcd123";
		String customBase = "http://semoss.org/ontologies";
		RdbmsTypeEnum dbType = RdbmsTypeEnum.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);
		
		// do the actual db loading
		RDBMSFlatCSVUploader reader = new RDBMSFlatCSVUploader();
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		
		ImportOptions options = new ImportOptions();
		
		options.setSMSSLocation(propWriter.propFileName);
		options.setDbName(engineName);
		options.setFileLocation(fileNames);
		options.setBaseUrl(customBase);
		options.setOwlFileLocation(owlFile);		
		options.setRDBMSDriverType(dbType);
		options.setAllowDuplicates(false);
		
		//reader.importFileWithOutConnection(propWriter.propFileName, engineName, fileNames, customBase, owlFile, dbType, false);
		reader.importFileWithOutConnection(options);
		
		// create the smss file and drop temp file
		File propFile = new File(propWriter.propFileName);
		File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
		FileUtils.copyFile(propFile, newProp);
		newProp.setReadable(true);
		FileUtils.forceDelete(propFile);
		
		long end = System.currentTimeMillis();

		System.out.println("TIME TO RUN: " + (end-start)/1000 + " seconds...");
		
	}
}
