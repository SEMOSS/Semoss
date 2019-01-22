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
import prerna.poi.main.helper.FileHelperUtil;
import prerna.poi.main.helper.ImportOptions;
import prerna.poi.main.helper.XLFileHelper;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

@Deprecated
public class RDBMSFlatExcelUploader extends AbstractFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSFlatExcelUploader.class.getName());
	
	// hold the existing rdbms structure
	private Map<String, Map<String, String>> existingRDBMSStructure;
	
	// this will hold the headers to the data types for each excel file
	private List<Map<String, Map<String, String[]>>> dataTypeMapList;
	private Map<String, Map<String, String[]>> dataTypeMap;
	
	// need to keep track of the new tables added when doing addToExisting
	private Map<String, String> newTables = new Hashtable<String, String>();
	
	// these keys are used within the return of the parse excel data to get the
	// headers and data types from a given excel file
	private XLFileHelper xlHelper;
	public static final String XL_HEADERS = "headers";
	public static final String XL_DATA_TYPES = "dataTypes";
	
	// used as a default for the unique row id
	private final String BASE_PRIM_KEY = "_UNIQUE_ROW_ID";

	// cases when we do not want to perform any cleaning on the values
	private boolean cleanString = true;

	/*
	 * Store the new user defined excel file names
	 * Format for this is:
	 * [	
	 * 		{
	 * 		excel_1_sheet_1 -> {
	 * 							fixed_header_name_1 -> user_changed_header_name_1,
	 *	 						fixed_header_name_2 -> user_changed_header_name_2,
	 * 							fixed_header_name_3 -> user_changed_header_name_3,
	 * 						}
	 * 		excel_1_sheet_2 -> {
	 * 							fixed_header_name_4 -> user_changed_header_name_4,
	 *	 						fixed_header_name_5 -> user_changed_header_name_5,
	 * 							fixed_header_name_6 -> user_changed_header_name_6,
	 * 						} 
	 * 		}, 
	 * 		{
	 * 		excel_2_sheet_1 -> {
	 * 							fixed_header_name_1 -> user_changed_header_name_1,
	 *	 						fixed_header_name_2 -> user_changed_header_name_2,
	 * 							fixed_header_name_3 -> user_changed_header_name_3,
	 * 						}
	 * 		excel_2_sheet_2 -> {
	 * 							fixed_header_name_4 -> user_changed_header_name_4,
	 *	 						fixed_header_name_5 -> user_changed_header_name_5,
	 * 							fixed_header_name_6 -> user_changed_header_name_6,
	 * 						} 
	 * 		}
	 * ]
	 */
	private List<Map<String, Map<String, String>>> userHeaderNames;
	private Map<String, Map<String, String>> excelHeaderNames;

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
		String appName = options.getDbName();
		String fileLocations = options.getFileLocations();
		String customBaseURI = options.getBaseUrl();
		String owlPath = options.getOwlFileLocation();		
		boolean allowDuplicates = options.isAllowDuplicates();
		cleanString = options.getCleanString();
		String appID = options.getEngineID();
		
		RdbmsTypeEnum dbDriverType = options.getRDBMSDriverType();
		queryUtil = SQLQueryUtil.initialize(dbDriverType);

		boolean error = false;
		// sets the custom base uri, sets the owl path, sets the smss location
		// and returns the fileLocations split into an array based on ';' character
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);
		LOGGER.setLevel(Level.WARN);
		try {
			// create the engine and the owler
			openRdbmsEngineWithoutConnection(appName, appID);
			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				// get the user defined headers if present
				if(userHeaderNames != null && !userHeaderNames.isEmpty()) {
					excelHeaderNames = userHeaderNames.get(i);
				}
				// similar to other csv reading
				// we load the user defined types
				if(dataTypeMapList != null && !dataTypeMapList.isEmpty()) {
					dataTypeMap = dataTypeMapList.get(i); 
				}
				processExcel(fileName, dataTypeMap);
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
			xlHelper.clear();
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
		boolean allowDuplicates = options.isAllowDuplicates();
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
				// get the user defined headers if present
				if(userHeaderNames != null && !userHeaderNames.isEmpty()) {
					excelHeaderNames = userHeaderNames.get(i);
				}
				// similar to other csv reading
				// we load the user defined types
				if(dataTypeMapList != null && !dataTypeMapList.isEmpty()) {
					dataTypeMap = dataTypeMapList.get(i); 
				}
				processExcel(fileName, dataTypeMap);
			}
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
			xlHelper.clear();
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
	 * @param fileLocation					The location of the excel file
	 * @throws SQLException 
	 */
	private void processExcel(final String FILE_LOCATION, Map<String, Map<String, String[]>> excelMeta) throws IOException {
		LOGGER.info("Processing excel file: " + FILE_LOCATION);

		// if excelMeta is null, we are not getting data type information from the FE, and we need to create this
		// using all the file info
		if(excelMeta == null) {
			// this is what will happen when the FE does not pass the BE the headers and data types to load
			// for a given excel file
			// assumption - the user wants all the things!!!
			excelMeta = parseExcelData(FILE_LOCATION);
		} else {
			// just open the connection to the file
			// and convert any user types to sql specific types for easy creation of tables
			parseExcel(FILE_LOCATION, excelMeta);
		}
		
		// alright, we have the file
		// now we need to loop through all the sheets and load as individual tables
		for(String sheetName : excelMeta.keySet()) {
			LOGGER.info("Processing excel sheet: " + sheetName);
			processSheet(sheetName, excelMeta.get(sheetName));	
		}
	}
	
	
	private void processSheet(String sheetName, Map<String, String[]> sheetMeta) throws IOException {
		/*
		 * We need to determine if we are going to create a new table or append onto an existing one
		 * Requirements for inserting into an existing table:
		 * 
		 * 1) If all the headers are contained within a single existing table,
		 * then we go ahead and insert into that table and keep everything else empty where columns
		 * might be missing
		 * 
		 * 2) The data types are the same within the excel file and the existing column in the table
		 */

		// TODO: I need to check my base assumption
		// need to get the updated rdbms after each sheet is loaded
		// since one sheet may just be a subset of another sheet in which we would append the information onto
		// ... should talk to someone and make sure that is right... or if we should just load as is because the 
		// user is smart and deliberately separated them out...
		existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine);
		
		// headers and data types arrays match based on position 
		String[] headers = sheetMeta.get(XL_HEADERS);
		String[] dataTypes = sheetMeta.get(XL_DATA_TYPES);
		
		String existingTableNameToInsert = null;
		
		// loop through every existing table
		TABLE_LOOP : for(String existingTableName : existingRDBMSStructure.keySet()) {
			// get the map containing the column names to data types for the existing table name
			Map<String, String> existingColTypeMap = existingRDBMSStructure.get(existingTableName);
			
			// if the number of headers does not match
			// we know it is not a good match
			if(existingColTypeMap.keySet().size() != headers.length) {
				// no way all columns are contained
				// look at the next table
				continue TABLE_LOOP;
			}
			
			// check that every header is contained in this table
			// check that the data types from the csv file and the table match
			for(int i = 0; i < headers.length; i++) {
				// existing rdbms structure returns with everything upper case
				String excelHeader = RDBMSEngineCreationHelper.cleanTableName(headers[i]).toUpperCase();
				String excelDataType = dataTypes[i].toUpperCase();
				
				if(!existingColTypeMap.containsKey(excelHeader)) {
					// if the column name doesn't exist in the existing table
					// look at the next table
					continue TABLE_LOOP;
				}
				
				// if we get here
				// we found a header that is in both
				String existingColDataType = existingColTypeMap.get(excelHeader.toUpperCase());
				
				// now test the data types
				// need to perform a non-exact match
				// i.e. float and double are the same, etc.
				if(!equivalentDataTypes(existingColDataType, excelDataType)) {
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
			bulkInsertSheet(sheetName, existingTableNameToInsert, sheetMeta);
		} else {
			// didn't find a table to insert into
			// create a new table
			
			// make the table name based on the sheet name
			String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(sheetName).toUpperCase();
			
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
			generateNewTableForExcelSheet(sheetName, uniqueTableName, sheetMeta);
		}		
	}
	
	///////////////////////////////// utility methods ///////////////////////////////
	

	/**
	 * Parse the excel file to get the header and data type for each column within each sheet
	 * @param fileLocation				The location of the excel file
	 * @return							Map containing the sheet to headers and data types, aligned by position
	 * 									Example of a return map:
	 * 									{
	 * 										"sheet1" -> {
	 * 													"headers" -> [Title, Genre, Movie_Budget],
	 * 													"dataTypes" -> [VARCHAR(800), VARCHAR(800), FLOAT];
	 * 													}
	 * 									}
	 * 									Index i of headers array matches to index i for dataTypes array
	 * 									for i = 0 to length of headers array
	 */
	private Map<String, Map<String, String[]>> parseExcelData(final String FILE_LOCATION) {
		LOGGER.info("Processing excel file: " + FILE_LOCATION);

		// use the excel file helper to load the data
		xlHelper = new XLFileHelper();
		xlHelper.parse(FILE_LOCATION);
		
		// lets get all the sheet names...
		// these will be the tables
		String[] sheetNames = xlHelper.getTables();
		
		// update header names to be those defined by user
		if(excelHeaderNames != null && !excelHeaderNames.isEmpty()) {
			xlHelper.modifyCleanedHeaders(excelHeaderNames);
		}
		
		// for each sheet we need to get the headers and data types for each column
		Map<String, Map<String, String[]>> excelMeta = new Hashtable<String, Map<String, String[]>>();
		for(String sheetName : sheetNames) {
			Map<String, String[]> sheetMeta = new Hashtable<String, String[]>();
			// get the headers from the sheet
			sheetMeta.put(XL_HEADERS, xlHelper.getHeaders(sheetName));
			// get the data types for the sheet
			sheetMeta.put(XL_DATA_TYPES, FileHelperUtil.generateDataTypeArrayFromPrediction(xlHelper.predictTypes(sheetName)));
			
			// store in excel meta
			excelMeta.put(sheetName, sheetMeta);
		}
		
		return excelMeta;
	}
	
	private void parseExcel(final String FILE_LOCATION, Map<String, Map<String, String[]>> excelMeta) {
		LOGGER.info("Processing excel file: " + FILE_LOCATION);

		// use the excel file helper to load the data
		xlHelper = new XLFileHelper();
		xlHelper.parse(FILE_LOCATION);
		
		// we need to convert from the generic data types from the FE to the sql specific types
		// need to do this for every sheet
		
		if(sqlHash.isEmpty()) {
			createSQLTypes();
		}
		
		// update header names to be those defined by user
		if(excelHeaderNames != null && !excelHeaderNames.isEmpty()) {
			xlHelper.modifyCleanedHeaders(excelHeaderNames);
		}
				
		for(String sheetName : excelMeta.keySet()) {
			// get the data types for this sheet as defined by user
			Map<String, String[]> sheetMeta = excelMeta.get(sheetName);
			String[] userDataTypes = sheetMeta.get(XL_DATA_TYPES);
			
			// convert to sql types
			int numCols = userDataTypes.length;
			String[] sqlDataTypes = new String[numCols];
			for(int colIdx = 0; colIdx < numCols; colIdx++) {
				if(sqlHash.containsKey(userDataTypes[colIdx])) {
					sqlDataTypes[colIdx] = sqlHash.get(userDataTypes[colIdx]);
				} else {
					sqlDataTypes[colIdx] = userDataTypes[colIdx];
				}
			}
			
			// put back into the hash so we have the data types there
			sheetMeta.put(XL_DATA_TYPES, sqlDataTypes);
		}
	}
	
	/**
	 * Create a new table using the excel sheet
	 * Add an additional identity column to the table to be the primary key (defined through the OWL, not through the database)
	 * @param SHEET_NAME						The name of the sheet in the excel file
	 * @param TABLE_NAME						The name of the table to create
	 * @param sheetMeta							Map containing the header and data type for each column, aligned by position
	 * @throws SQLException 
	 * @throws IOException 
	 */
	private void generateNewTableForExcelSheet(final String SHEET_NAME, final String TABLE_NAME, Map<String, String[]> sheetMeta) throws IOException {
		LOGGER.info("Creating a new table from " + SHEET_NAME);

		// TODO: should this be static across all tables or made to be different?
		final String UNIQUE_ROW_ID = TABLE_NAME + BASE_PRIM_KEY;
		
		// first create the table
		// make sure it has the identify column
		createTable(TABLE_NAME, sheetMeta, UNIQUE_ROW_ID);
		// this logic will be to do a bulk insert
		bulkInsertSheet(SHEET_NAME, TABLE_NAME, sheetMeta);
		
		// note: now doing this on table creation
		// add the unique id for the table
//		addIdentityColumnToTable(TABLE_NAME, UNIQUE_ROW_ID);
		
		// now need to add the table onto the owl file
		addTableToOWL(TABLE_NAME, UNIQUE_ROW_ID, sheetMeta);
	}

	/**
	 * Creates a table using the table name and columns/types specified in sheetMeta
	 * @param TABLE_NAME				The name of the table
	 * @param sheetMeta					Map containing the headers and column types
	 * 									for the excel sheet being loaded
	 */
	private void createTable(final String TABLE_NAME, Map<String, String[]> sheetMeta, final String UNIQUE_ROW_ID) {
		// headers and data types arrays match based on position 
		String[] headers = sheetMeta.get(XL_HEADERS);
		String[] dataTypes = sheetMeta.get(XL_DATA_TYPES);
		
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
	 * Generates a table and does a bulk insert of all the data from the excel sheet
	 * @param SHEET_NAME					
	 * @param TABLE_NAME
	 * @param sheetMeta
	 * @throws SQLException 
	 */
	private void bulkInsertSheet(final String SHEET_NAME, final String TABLE_NAME, Map<String, String[]> sheetMeta) throws IOException {
		// headers and data types arrays match based on position 
		String[] headers = sheetMeta.get(XL_HEADERS);
		String[] dataTypes = sheetMeta.get(XL_DATA_TYPES);
		
		// now we need to loop through the excel sheet and cast to the appropriate type and insert
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
		Object[] nextRow = null;
		try {
			int[] headerIndicesToGet = this.xlHelper.getHeaderIndicies(SHEET_NAME, headers);
			while( (nextRow  = this.xlHelper.getNextRow(SHEET_NAME, headerIndicesToGet)) != null ) {
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = dataTypes[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex].toString());
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						} else {
							// set default as null
							ps.setObject(colIndex+1, null);
						}
					} else if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT") || type.equalsIgnoreCase("LONG")) {
						Double value = null;
						String val = nextRow[colIndex].toString().trim();
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
							String value = Utility.cleanString(nextRow[colIndex].toString(), false);
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
	private void addTableToOWL(final String TABLE_NAME, final String IDENTITY_COL_NAME, Map<String, String[]> sheetMeta) {
		// headers and data types arrays match based on position 
		String[] headers = sheetMeta.get(XL_HEADERS);
		String[] dataTypes = sheetMeta.get(XL_DATA_TYPES);
				
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
	
	/**
	 * Set the data types and headers to load for each csv file
	 * @param dataTypeMap
	 */
	public void setDataTypeMapList(List<Map<String, Map<String, String[]>>> dataTypeMapList) {
		this.dataTypeMapList = dataTypeMapList;
	}
	
	public void setNewExcelHeaders(List<Map<String, Map<String, String>>> newExcelHeaders) {
		this.userHeaderNames = newExcelHeaders;
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
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Movie_Table.xlsx";
		
		// set a bunch of db stuff
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String engineName = "abcd1234";
		String customBase = "http://semoss.org/ontologies";
		RdbmsTypeEnum dbType = RdbmsTypeEnum.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", ImportOptions.DB_TYPE.RDBMS);
		
		// do the actual db loading
		RDBMSFlatExcelUploader reader = new RDBMSFlatExcelUploader();
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