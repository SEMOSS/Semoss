package prerna.poi.main;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSFlatCSVUploader extends AbstractFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSFlatCSVUploader.class.getName());
	
	private Map<String, Map<String, String>> existingRDBMSStructure;
	
	
	// these keys are used within the return of the parseCSVData to get the
	// headers and data types from a given csv file
	private final String CSV_HEADERS = "headers";
	private final String CSV_DATA_TYPES = "dataTypes";

	/**
	 * Load a new flat table rdbms database
	 * @param smssLocation						The location of the smss file
	 * @param fileLocations						The location of the files
	 * @param customBaseURI						customBaseURI to set... need to remove this stupid thing
	 * @param owlPath							The path to the owl file
	 * @param dbName							The name of the database driver
	 * @param dbDriverType						The database type (h2, mysql, etc.)
	 * @param allowDuplicates					Boolean to determine if we should delete duplicate rows
	 * @return
	 * @throws IOException 
	 */
	public IEngine importFileWithOutConnection(String smssLocation, String fileLocations, String customBaseURI, String owlPath, String engineName, SQLQueryUtil.DB_TYPE dbDriverType, boolean allowDuplicates) throws IOException {
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
				processTable(fileName);
			}
			// write the owl file
			createBaseRelations();
			// create the base question sheet
			RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engine, queryUtil);
		} finally {
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
	 */
	private void processTable(final String FILE_LOCATION) {
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
				String csvHeader = RDBMSEngineCreationHelper.cleanTableName(headers[i]).toUpperCase();
				String csvDataType = dataTypes[i].toUpperCase();
				// existing rdbms structure returns with everything upper case
				if(!existingColTypeMap.containsKey(csvHeader.toUpperCase())) {
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
			String fileName = getOriginalFileName(FILE_LOCATION);
			// make the table name based on the fileName
			String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName);
			
			// we also need to make sure the table is unique
			if(existingRDBMSStructure.containsKey(cleanTableName.toUpperCase())) {
				// TODO: might want a better way to do this
				// 		but i think this is pretty unlikely... maybe...
				cleanTableName += "_2"; 
			}
			
			// run the process to create a new table from the csv file
			generateNewTableFromCSV(FILE_LOCATION, cleanTableName, csvMeta);
			
			
			//TODO: this is where join information would need to be added
			// 		if we want the new table to be able to be joined onto existing tables
			
			
			
			
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
		CSVFileHelper helper = new CSVFileHelper();
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

		// close the helper
		helper.clear();
		
		Map<String, String[]> csvMeta = new Hashtable<String, String[]>();
		csvMeta.put(CSV_HEADERS, headers);
		csvMeta.put(CSV_DATA_TYPES, dataTypes);
		
		return csvMeta;
	}
	
	/**
	 * Take the file location and return the original file name
	 * Based on upload flow, files that go through FileUploader.java class get appended with the date of upload
	 * in the format of "_yyyy_MM_dd_HH_mm_ss_SSSS" (length of 25) 
	 * Thus, we see if the file has the date appended and remove it if we find it
	 * @param FILE_LOCATION						The location of the csv file
	 * @return									The original file name of the csv file
	 */
	private String getOriginalFileName(final String FILE_LOCATION) {
		// before this point is reached, the FileUploader appends the time as "_yyyy_MM_dd_HH_mm_ss_SSSS"
		// onto the original fileName in order to ensure that it is unique
		// since we are using the fileName to be the table name, let us try and remove this
		String fileName = new File(FILE_LOCATION).getName().replace(".csv", "");
		// 24 is the length of the date being added
		if(fileName.length() > 28) {
			String fileEnd = fileName.substring(fileName.length()-24);
			try {
				new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").parse(fileEnd);
				// if the parsing was successful, we remove it from the fileName
				// it is -25 because of the 24 above plus one more for the "_"
				fileName = fileName.substring(0, fileName.length()-25);
			} catch (ParseException e) {
				// the end was not the added date, so do nothing
			}
		}
		return fileName;
	}
	
	/**
	 * Create a new table using the csv file
	 * Add an additional identity column to the table to be the primary key (defined through the OWL, not through the database)
	 * @param FILE_LOCATION						The location of the csv file
	 * @param TABLE_NAME						The name of the table to create
	 * @param csvMeta							Map containing the header and data type for each column, aligned by position
	 */
	private void generateNewTableFromCSV(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) {
		LOGGER.info("Creating a new table from " + FILE_LOCATION);

		String insertCSVIntoTableQuery = generateCreateTableFromCSVSQL(FILE_LOCATION, TABLE_NAME, csvMeta);
		// load the csv as a table
		System.out.println(insertCSVIntoTableQuery);
		this.engine.insertData(insertCSVIntoTableQuery);
		
		// now we need to append an identity column for the table, this will be the prim key
		// TODO: should this be static across all tables or made to be different?
		final String UNIQUE_ROW_ID = TABLE_NAME + "_UNIQUE_ROW_ID";
		
		String addIdentityColumnQuery = addIdentityColumnToTable(TABLE_NAME, UNIQUE_ROW_ID);

		// add the unique id for the table
		System.out.println(addIdentityColumnQuery);
		this.engine.insertData(addIdentityColumnQuery);
		
		// now need to add the table onto the owl file
		addTableToOWL(TABLE_NAME, UNIQUE_ROW_ID, csvMeta);
	}
	
	
	/**
	 * Inserts the csv data into an existing table
	 * @param FILE_LOCATION						The location of the csv file
	 * @param TABLE_NAME						The name of the table to insert the csv data into
	 * @param csvMeta							Map containing the header and data type for each column, aligned by position
	 */
	private void insertCSVIntoExistingTable(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) {
		/*
		 * This will be done in 3 steps
		 * 1) load csv file into a temp table
		 * 2) insert the temp table into the existing table
		 * 3) drop the temp table
		 */
		
		final String TEMP_TABLE = "TEMP_TABLE_98712396874";
		// 1) create the temp table
		String createTempTableFromCSVQuery = generateCreateTableFromCSVSQL(FILE_LOCATION, TEMP_TABLE , csvMeta);
		System.out.println(createTempTableFromCSVQuery);
		this.engine.insertData(createTempTableFromCSVQuery);
		
		// 2) create query to insert temp into existing table
		String insertTableIntoExistingTableQuery = generateInsertTableIntoOtherTableSQL(TABLE_NAME, TEMP_TABLE, csvMeta);
		System.out.println(insertTableIntoExistingTableQuery);
		this.engine.insertData(insertTableIntoExistingTableQuery);
		
		// 3) drop current table
		this.engine.removeData("DROP TABLE " + TEMP_TABLE);
	}
	
	/**
	 * Generate the query to create a new table with the csv data
	 * @param FILE_LOCATION				The location of the csv file
	 * @param TABLE_NAME				The name of the table to load the csv file into
	 * @param csvMeta					The column names and types of the csv file
	 * @return							The query to load the csv file into the table
	 */
	private String generateCreateTableFromCSVSQL(final String FILE_LOCATION, final String TABLE_NAME, Map<String, String[]> csvMeta) {
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
		
		return queryBuilder.toString();
	}
	
	/**
	 * Generate the query to create a new table with the csv data
	 * @param FILE_LOCATION				The location of the csv file
	 * @param TABLE_NAME				The name of the table to load the csv file into
	 * @param csvMeta					The column names and types of the csv file
	 * @return							The query to load the csv file into the table
	 */
	private String generateInsertTableIntoOtherTableSQL(final String BASE_TABLE, final String TABLE_TO_INSERT, Map<String, String[]> csvMeta) {
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
		
		return queryBuilder.toString();
	}
	
	/**
	 * Generate the query to append an identity column onto the table
	 * @param TABLE_NAME				The name of the table
	 * @param IDENTITY_COL_NAME			The name of the identity column
	 * @return
	 */
	private String addIdentityColumnToTable(final String TABLE_NAME, final String IDENTITY_COL_NAME) {
		StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
		queryBuilder.append(TABLE_NAME);
		queryBuilder.append(" ADD ").append(IDENTITY_COL_NAME).append(" IDENTITY");
		
		return queryBuilder.toString();
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
		// if one is a float and the other a double
		if( (existingDataType.contains("FLOAT") && csvDataType.contains("DOUBLE") ) || 
				( existingDataType.contains("DOUBLE") && csvDataType.contains("FLOAT") ) ) {
			return true;
		}
		
		//TODO: need to expand this to include other things.... too lazy to do this right now
		
		return false;
	}
	
	
	//////////////////////////////// end utility methods //////////////////////////////
	

	///////////////////////////////// test methods /////////////////////////////////
	
	public static void main(String[] args) throws IOException {
		// run this test when the server is not running
		// this will create the db and smss file so when you start the server
		// the database created will be picked up and exposed
		TestUtilityMethods.loadDIHelper();

		// set the file
		String fileNames = "C:\\Users\\mahkhalil\\Desktop\\Movie Results.csv;C:\\Users\\mahkhalil\\Desktop\\Movie Results.csv;C:\\Users\\mahkhalil\\Desktop\\Movie Characteristics.csv";
		
		// set a bunch of db stuff
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String engineName = "testing4";
		String customBase = "http://semoss.org/ontologies";
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;

		// write .temp to convert to .smss
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setRDBMSType(dbType);
		propWriter.runWriter(engineName, "", "", "" ,ImportDataProcessor.DB_TYPE.RDBMS);
		
		// do the actual db loading
		RDBMSFlatCSVUploader reader = new RDBMSFlatCSVUploader();
		String owlFile = baseFolder + "/" + propWriter.owlFile;
		reader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBase, owlFile, engineName, dbType, false);
		
		// create the smss file and drop temp file
		File propFile = new File(propWriter.propFileName);
		File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
		FileUtils.copyFile(propFile, newProp);
		newProp.setReadable(true);
		FileUtils.forceDelete(propFile);
	}
	
}
