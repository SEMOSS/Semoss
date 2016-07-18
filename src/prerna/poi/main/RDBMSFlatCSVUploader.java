package prerna.poi.main;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
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
import prerna.util.sql.SQLQueryUtil;

public class RDBMSFlatCSVUploader extends AbstractCSVFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSFlatCSVUploader.class.getName());
	
	// hold the existing rdbms structure
	private Map<String, Map<String, String>> existingRDBMSStructure;
	
	// need to keep track of the new tables added when doing addToExisting
	private Set<String> newTables = new HashSet<String>();
	
	// these keys are used within the return of the parseCSVData to get the
	// headers and data types from a given csv file
	private final String CSV_HEADERS = "headers";
	private final String CSV_DATA_TYPES = "dataTypes";
	
	// used as a default for the unique row id
	private final String BASE_PRIM_KEY = "_UNIQUE_ROW_ID";
	
	// CSVFileHelper to get information from the csv file
	// i.e. the headers and data types
	// also used to look through csv file when performing bulk
	// inserts instead of CSV file upload - used currently
	// when there are date data types since they must be in a specific
	// format for RDBMS (probably differs based on type.. right now assuming
	// it is h2)
	private CSVFileHelper helper;
	
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
	 * @throws SQLException 
	 */
	public IEngine importFileWithOutConnection(String smssLocation, String engineName, String fileLocations, String customBaseURI, String owlPath, SQLQueryUtil.DB_TYPE dbDriverType, boolean allowDuplicates) throws IOException, SQLException {
		boolean error = false;
		queryUtil = SQLQueryUtil.initialize(dbDriverType);
		// sets the custom base uri, sets the owl path, sets the smss location
		// and returns the fileLocations split into an array based on ';' character
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);
		LOGGER.setLevel(Level.WARN);
		try {
			// create the engine and the owler
			openRdbmsEngineWithoutConnection(engineName);
			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				if(i == 0) {
					existingRDBMSStructure = new Hashtable<String, Map<String, String>>();
				} else {
					// need to update to get the rdbms structure to determine how the new files should be added
					existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine, queryUtil);
				}
				// similar to other csv reading
				// we load the types into rdfMap
				if(!propFileExist){
					rdfMap = rdfMapArr[i];
				}
				processTable(fileName);
			}
			// write the owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine, queryUtil);
		} finally {
			// close the helper
			helper.clear();
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
	 * @throws SQLException 
	 */
	public void importFileWithConnection(String smssLocation, String fileLocations, String customBaseURI, String owlPath, prerna.util.sql.SQLQueryUtil.DB_TYPE dbDriverType, boolean allowDuplicates) throws IOException {
		boolean error = false;
		
		queryUtil = SQLQueryUtil.initialize(dbDriverType);
		// sets the custom base uri, sets the owl path, sets the smss location
		// and returns the fileLocations split into an array based on ';' character
		String[] files = prepareReader(fileLocations, customBaseURI, owlPath, smssLocation);

		LOGGER.setLevel(Level.WARN);
		try {
			// create the engine and the owler
			openEngineWithConnection(smssLocation);
			for(int i = 0; i < files.length;i++)
			{
				String fileName = files[i];
				// cause of stupid split adding empty values
				if(fileName.isEmpty()) {
					continue;
				}
				// need to update to get the rdbms structure to determine how the new files should be added
				existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine, queryUtil);
				
				processTable(fileName);
			}
			// write the owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.addToExistingQuestionFile(this.engine, newTables, queryUtil);
		} finally {
			// close the helper
			helper.clear();
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
	 * 
	 * 
	 * @param fileLocation					The location of the csv file
	 * @throws SQLException 
	 */
	private void processTable(final String FILE_LOCATION) throws IOException {
		// parse the csv meta to get the headers and data types
		// headers and data types arrays match based on position 
		// currently assume we are loading all the columns
		// currently not taking in the dataTypes
		Map<String, String[]> csvMeta = parseCSVData(FILE_LOCATION);
		
		/*
		 * We need to determine if we are going to create a new table or append onto an existing engine
		 * Requirements for inserting into an existing table:
		 * 
		 * 1) If all the headers are a subset of the existing headers within a single table,
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
			if(existingRDBMSStructure.containsKey(cleanTableName)) {
				// TODO: might want a better way to do this
				// 		but i think this is pretty unlikely... maybe...
				cleanTableName += "_2"; 
			}
			
			// run the process to create a new table from the csv file
			generateNewTableFromCSV(FILE_LOCATION, cleanTableName, csvMeta);
			
			
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
		helper = new CSVFileHelper();
		// assume csv
		helper.setDelimiter(',');
		helper.parse(FILE_LOCATION);
		
		// currently assume we are loading all the columns
		// currently not taking in the dataTypes
		// ... at least this way, we know all the values always work with regards to loading
		String[] headers = helper.getHeaders();
		LOGGER.info("Found headers: " + Arrays.toString(headers));
		String[] dataTypes = helper.predictTypes();
		LOGGER.info("Found data types: " + Arrays.toString(dataTypes));

		Map<String, String[]> csvMeta = new Hashtable<String, String[]>();
		csvMeta.put(CSV_HEADERS, headers);
		csvMeta.put(CSV_DATA_TYPES, dataTypes);
		
		return csvMeta;
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
		
		if(containsDateDataType(csvMeta.get(CSV_DATA_TYPES))) {
			// we had a date!
			// first create the table
			createTable(TABLE_NAME, csvMeta);
			// this logic will be to do a bulk insert
			bulkInsertCSVFile(FILE_LOCATION, TABLE_NAME, csvMeta);
		} else {
			// this logic just grabs the csv file and creates the table using it all in one go
			generateCreateTableFromCSVSQL(FILE_LOCATION, TABLE_NAME, csvMeta);
		}
		
		// now we need to append an identity column for the table, this will be the prim key
		// TODO: should this be static across all tables or made to be different?
		final String UNIQUE_ROW_ID = TABLE_NAME + BASE_PRIM_KEY;
		// add the unique id for the table
		addIdentityColumnToTable(TABLE_NAME, UNIQUE_ROW_ID);
		
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
		
		if(containsDateDataType(csvMeta.get(CSV_DATA_TYPES))) {
			// perform a bulk insert into the table
			bulkInsertCSVFile(FILE_LOCATION, TABLE_NAME, csvMeta);
		} else {
			// no date found... lets do steps 1-3
			final String TEMP_TABLE = "TEMP_TABLE_98712396874";
			// 1) create the temp table
			generateCreateTableFromCSVSQL(FILE_LOCATION, TEMP_TABLE, csvMeta);
			// 2) create query to insert temp into existing table
			insertTableIntoOtherTable(TABLE_NAME, TEMP_TABLE, csvMeta);
			// 3) drop current table
			this.engine.removeData("DROP TABLE " + TEMP_TABLE);
		}
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
		this.engine.insertData(queryBuilder.toString());
	}
	
	/**
	 * Creates a table using the table name and columns/types specified in csvMeta
	 * @param TABLE_NAME				The name of the table
	 * @param csvMeta					Map containing the headers and column types
	 * 									for the table
	 */
	private void createTable(final String TABLE_NAME, Map<String, String[]> csvMeta) {
		// headers and data types arrays match based on position 
		String[] headers = csvMeta.get(CSV_HEADERS);
		String[] dataTypes = csvMeta.get(CSV_DATA_TYPES);
		
		// need to first create the table
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
		queryBuilder.append(")");
		LOGGER.info("CREATE TABLE QUERY : " + queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
	}
	
	
	/**
	 * Generates a table and does a bulk insert of all the data from teh csv file
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
			while( (nextRow  = this.helper.getNextRow()) != null ) {
				// we need to loop through every value and cast appropriately
				for(int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					String type = dataTypes[colIndex];
					if(type.equalsIgnoreCase("DATE")) {
						java.util.Date value = Utility.getDateAsDateObj(nextRow[colIndex]);
						if(value != null) {
							ps.setDate(colIndex+1, new java.sql.Date(value.getTime()));
						}
					} else if(type.equalsIgnoreCase("DOUBLE")) {
						Double value = Utility.getDouble(nextRow[colIndex]);
						if(value != null) {
							ps.setDouble(colIndex+1, value);
						}
					} else {
						String value = nextRow[colIndex];
						ps.setString(colIndex+1, value + "");
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
			String errorMessage = "Error occured while performing insert on csv data row:"
					+ "\n" + Arrays.toString(nextRow);
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
		this.engine.insertData(queryBuilder.toString());
	}
	
	/**
	 * Generate the query to append an identity column onto the table
	 * @param TABLE_NAME				The name of the table
	 * @param IDENTITY_COL_NAME			The name of the identity column
	 */
	private void addIdentityColumnToTable(final String TABLE_NAME, final String IDENTITY_COL_NAME) {
		StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" ADD ").append(IDENTITY_COL_NAME).append(" IDENTITY");
		
		System.out.println(queryBuilder.toString());
		this.engine.insertData(queryBuilder.toString());
	}
	
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
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", "", ImportOptions.DB_TYPE.RDBMS);
		
		// do the actual db loading
		RDBMSFlatCSVUploader reader = new RDBMSFlatCSVUploader();
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		reader.importFileWithOutConnection(propWriter.propFileName, engineName, fileNames, customBase, owlFile, dbType, false);
		
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
