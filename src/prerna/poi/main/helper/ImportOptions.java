package prerna.poi.main.helper;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.util.sql.RdbmsTypeEnum;

public class ImportOptions {

	/**
	 * This class is set as a wrapper such that you do not need to know the specific keys for each of these import options
	 * but can grab and set the values you want
	 */
	
	private Hashtable<ImportOptions.IMPORT_OPTIONS , Object> thisMap = new Hashtable<ImportOptions.IMPORT_OPTIONS , Object>();
	
	/*
	 * Keep the enums all caps for consistency
	 */
	public enum IMPORT_METHOD {CREATE_NEW, ADD_TO_EXISTING, CONNECT_TO_EXISTING_RDBMS}; // OVERRIDE
	public enum IMPORT_TYPE {CSV, NLP, EXCEL_POI, EXCEL, OCR, CSV_FLAT_LOAD, EXCEL_FLAT_UPLOAD, EXTERNAL_RDBMS};
	public enum DB_TYPE {RDF, RDBMS, TINKER, SOLR};
	public enum TINKER_DRIVER {TG, XML, JSON, NEO4J};
	
	public enum IMPORT_OPTIONS {
		SMSS_LOCATION, 			// should point to string - location of smss file
		OWL_FILE_LOCATION,
		IMPORT_METHOD, 			// should point to IMPORT_METHOD - either create new, add to existing, override
		IMPORT_TYPE, 			// should point to ImportDataProcessor.IMPORT_TYPE - csv, nlp, etc.
		DB_TYPE, 				// should point to ImportDataProcess.DB_TYPE - rdf or rdbms
		FILE_LOCATION, 			// should point to string - the file paths as a single string which are ";" delimited
		BASE_URL, 				// should point to string - the base URI for the db (http://health.mil/ontologies for MHS)
		DB_NAME, 				// should point to string - either the name of the new DB to create or the existing DB to alter
		BASE_FOLDER,			// should point to string - the base workspace location on the machine
		DB_DRIVER_TYPE, 		// should point to SQLQueryUtil.DB_TYPE - only valid for RDBMS where we need to specify the type
		ALLOW_DUPLICATES, 		// should point to boolean - only valid for RDBMS where we should remove duplicate values in tables
		AUTO_LOAD, 				// should point to boolean - determine if we should load the database directly or shut it down and have
		CLEAN_STRING, 			// TODO: only used by flat upload at the moment -> control if we should clean string values upon insertion
		
								// have the smss watcher find the engine and load it
		DEFINED_METAMODEL,		// should point to Hashtable<String, String>[] - only valid for csv upload
		DEFINED_PROPERTY_FILES,	// should be a ";" delimited string array containing the full path to the files
		CSV_DATA_TYPE_MAP,		// should point to Map<String, Map<String, String>> - only valid for flat upload
		EXCEL_DATA_TYPE_MAP,	// should point to List<Map<String, Map<String, String>> - only valid for flat upload
		CSV_DATA_NEW_HEADERS,	// should point to Map<String, Map<String, String>
		EXCEL_DATA_NEW_HEADERS,	// should point to List<Map<String, Map<String, String>>>
		HOST,
		PORT,
		SCHEMA,
		USERNAME,
		PASSWORD,
		ADDITIONAL_JDBC_PROPERTIES,
		EXTERNAL_METAMODEL,
		OBJECT_VALUE_MAP,		// RDBMSReader will look in these maps for objects that are specified in the prop file,
								// but do not exist as headers in a CSV
		OBJECT_TYPE_MAP,		
		ROW_KEY,				// What to put in a prop file to grab the current row number
		CREATE_INDEXES,			// If true, RDBMSReader will create indexes when cleaning up tables
		TINKER_DRIVER_TYPE,		//.tg, neo4j, .xml, .json
		ENGINE_ID,
		CSV_DATATYPES           // used for defining a metamodel
	};
	
	/**
	 * Set default values when instantiating
	 */
	public ImportOptions() {
		setDefaultValues();
	}
	
	/**
	 * Set defaults
	 */
	private void setDefaultValues() {
		
		// Set these as empty maps to begin with,
		// So that classes can check objectValueMap.containsKey(key) without throwing an error
		setObjectValueMap(new HashMap<String, String>());
		setObjectTypeMap(new HashMap<String, String>());
		
		// By default, create indexes (business as usual)
		// In some special cases, a class may want to set this to false,
		// For example, when uploading several files in succession
		setCreateIndexes(true);
	}
	
	///////////////////////////// getters & setters /////////////////////////////////////
	
	/**
	 * Get the smss location
	 * @return
	 */
	public String getSMSSLocation() {
		return (String) thisMap.get(IMPORT_OPTIONS.SMSS_LOCATION);
	}
	
	public void setSMSSLocation(String smssLocation) {
		thisMap.put(IMPORT_OPTIONS.SMSS_LOCATION, smssLocation);
	}
	
	public String getOwlFileLocation() {
		return (String) thisMap.get(IMPORT_OPTIONS.OWL_FILE_LOCATION);
	}
	
	public void setOwlFileLocation(String owlFileLocation) {
		thisMap.put(IMPORT_OPTIONS.OWL_FILE_LOCATION, owlFileLocation);
	}
	/**
	 * Get the import method
	 * @return
	 */
	public ImportOptions.IMPORT_METHOD getImportMethod() {
		return (ImportOptions.IMPORT_METHOD) thisMap.get(IMPORT_OPTIONS.IMPORT_METHOD);
	}
	
	public void setImportMethod(ImportOptions.IMPORT_METHOD importMethod) {
		thisMap.put(IMPORT_OPTIONS.IMPORT_METHOD, importMethod);
	}
	
	/**
	 * Get the import type
	 * @return
	 */
	public ImportOptions.IMPORT_TYPE getImportType() {
		return (ImportOptions.IMPORT_TYPE) thisMap.get(IMPORT_OPTIONS.IMPORT_TYPE);
	}
	
	public void setImportType(ImportOptions.IMPORT_TYPE importType) {
		thisMap.put(IMPORT_OPTIONS.IMPORT_TYPE, importType);
	}
	
	
	/**
	 * Get the database type
	 * @return
	 */
	public ImportOptions.DB_TYPE getDbType() {
		return (ImportOptions.DB_TYPE) thisMap.get(IMPORT_OPTIONS.DB_TYPE);
	}
	
	public void setDbType(ImportOptions.DB_TYPE dbType) {
		thisMap.put(IMPORT_OPTIONS.DB_TYPE, dbType);
	}
	
	/**
	 * Get the file location - semicolon delimited
	 * @return
	 */
	public String getFileLocations() {
		return (String) thisMap.get(IMPORT_OPTIONS.FILE_LOCATION);
	}
	
	public void setFileLocation(String fileLocation) {
		thisMap.put(IMPORT_OPTIONS.FILE_LOCATION, fileLocation);
	}
	
	/**
	 * Get the base url
	 * @return
	 */
	public String getBaseUrl() {
		return (String) thisMap.get(IMPORT_OPTIONS.BASE_URL);
	}
	
	public void setBaseUrl(String baseUrl) {
		thisMap.put(IMPORT_OPTIONS.BASE_URL, baseUrl);
	}
	
	/**
	 * Get the database name
	 * @return
	 */
	public String getDbName() {
		return (String) thisMap.get(IMPORT_OPTIONS.DB_NAME);
	}
	
	public void setDbName(String dbName) {
		thisMap.put(IMPORT_OPTIONS.DB_NAME, dbName);
	}
	
	/**
	 * Get the base folder to determine where to add db folders
	 * @return
	 */
	public String getBaseFolder() {
		return (String) thisMap.get(IMPORT_OPTIONS.BASE_FOLDER);
	}
	
	public void setBaseFolder(String baseFolder) {
		thisMap.put(IMPORT_OPTIONS.BASE_FOLDER, baseFolder);
	}
	
	/**
	 * Get the specific type of RDBMS engine
	 * @return
	 */
	public RdbmsTypeEnum getRDBMSDriverType() {
		return (RdbmsTypeEnum) thisMap.get(IMPORT_OPTIONS.DB_DRIVER_TYPE);
	}
	
	
	public void setRDBMSDriverType(RdbmsTypeEnum rdbmsDriverType) {
		thisMap.put(IMPORT_OPTIONS.DB_DRIVER_TYPE, rdbmsDriverType);
	}
	
	public void setTinkerDriverType(TINKER_DRIVER driver) {
		thisMap.put(IMPORT_OPTIONS.TINKER_DRIVER_TYPE, driver);
	}
	
	public TINKER_DRIVER getTinkerDriverType() {
		return (TINKER_DRIVER) thisMap.get(IMPORT_OPTIONS.TINKER_DRIVER_TYPE);
	}
	
	/**
	 * Get if we should allow duplicates within the rdbms tables
	 * @return
	 */
	public Boolean isAllowDuplicates() {
		return (Boolean) thisMap.get(IMPORT_OPTIONS.ALLOW_DUPLICATES);
	}
	
	public void setAllowDuplicates(Boolean allowDuplicates) {
		thisMap.put(IMPORT_OPTIONS.ALLOW_DUPLICATES, allowDuplicates);
	}
	
	/**
	 * Get if we should load the engine directly or close it and have the watcher load it
	 * @return
	 */
	public Boolean isAutoLoad() {
		return (Boolean) thisMap.get(IMPORT_OPTIONS.AUTO_LOAD);
	}
	
	public void setAutoLoad(boolean autoLoad) {
		thisMap.put(IMPORT_OPTIONS.AUTO_LOAD, autoLoad);
	}
	
	/**
	 * Get the metamodel for a set of csv files
	 * @return
	 */
	public Hashtable<String, String>[] getMetamodelArray() {
		return (Hashtable<String, String>[]) thisMap.get(IMPORT_OPTIONS.DEFINED_METAMODEL);
	}
	
	public void setMetamodelArray(Hashtable<String, String>[] metamodelArray) {
		thisMap.put(IMPORT_OPTIONS.DEFINED_METAMODEL, metamodelArray);
	}
	
	/**
	 * Get the metamodel for a set of csv files
	 * @return
	 */
	public String getPropertyFiles() {
		return (String) thisMap.get(IMPORT_OPTIONS.DEFINED_PROPERTY_FILES);
	}
	
	public void setPropertyFiles(String propertyFiles) {
		thisMap.put(IMPORT_OPTIONS.DEFINED_PROPERTY_FILES, propertyFiles);
	}
	
	/**
	 * Get the dataTypeMap for a set of csv files
	 * @return
	 */
	public List<Map<String, String[]>> getCsvDataTypeMap() {
		return (List<Map<String, String[]>>) thisMap.get(IMPORT_OPTIONS.CSV_DATA_TYPE_MAP);
	}
	
	public void setCsvDataTypeMap(List<Map<String, String[]>> dataTypeMap) {
		thisMap.put(IMPORT_OPTIONS.CSV_DATA_TYPE_MAP, dataTypeMap);
	}
	
	/**
	 * Get the dataTypeMap for a set of excel files
	 * @return
	 */
	public List<Map<String, Map<String, String[]>>> getExcelDataTypeMap() {
		return (List<Map<String, Map<String, String[]>>>) thisMap.get(IMPORT_OPTIONS.EXCEL_DATA_TYPE_MAP);
	}
	
	public void setExcelDataTypeMap(List<Map<String, Map<String, String[]>>> dataTypeMap) {
		thisMap.put(IMPORT_OPTIONS.EXCEL_DATA_TYPE_MAP, dataTypeMap);
	}
	
	/**
	 * Get the user defined changed headers for a set of csv files
	 * @return
	 */
	public Map<String, Map<String, String>> getCsvNewHeaders() {
		return (Map<String, Map<String, String>>) thisMap.get(IMPORT_OPTIONS.CSV_DATA_NEW_HEADERS);
	}
	
	public void setCsvNewHeaders(Map<String, Map<String, String>> newCsvHeaders) {
		thisMap.put(IMPORT_OPTIONS.CSV_DATA_NEW_HEADERS, newCsvHeaders);
	}
	
	/**
	 * Get the user defined changed headers for a set of excel files
	 * @return
	 */
	public List<Map<String, Map<String, String>>> getExcelNewHeaders() {
		return (List<Map<String, Map<String, String>>>) thisMap.get(IMPORT_OPTIONS.EXCEL_DATA_NEW_HEADERS);
	}
	
	public void setExcelNewHeaders(List<Map<String, Map<String, String>>> newExcelHeaders) {
		thisMap.put(IMPORT_OPTIONS.EXCEL_DATA_NEW_HEADERS, newExcelHeaders);
	}
	
	public String getHost() {
		return (String) thisMap.get(IMPORT_OPTIONS.HOST);
	}
	
	public void setHost(String host) {
		thisMap.put(IMPORT_OPTIONS.HOST, host);
	}
	
	public String getPort() {
		return (String) thisMap.get(IMPORT_OPTIONS.PORT);
	}
	
	public void setPort(String port) {
		thisMap.put(IMPORT_OPTIONS.PORT, port);
	}
	
	public String getSchema() {
		return (String) thisMap.get(IMPORT_OPTIONS.SCHEMA);
	}
	
	public void setSchema(String schema) {
		thisMap.put(IMPORT_OPTIONS.SCHEMA, schema);
	}
	
	public String getUsername() {
		return (String) thisMap.get(IMPORT_OPTIONS.USERNAME);
	}
	
	public void setUsername(String username) {
		thisMap.put(IMPORT_OPTIONS.USERNAME, username);
	}
	
	public String getPassword() {
		return (String) thisMap.get(IMPORT_OPTIONS.PASSWORD);
	}
	
	public void setPassword(String password) {
		thisMap.put(IMPORT_OPTIONS.PASSWORD, password);
	}
	
	public String getAdditionalJDBCProperties() {
		return (String) thisMap.get(IMPORT_OPTIONS.ADDITIONAL_JDBC_PROPERTIES);
	}
	
	public void setAdditionalJDBCProperties(String additionalProperties) {
		thisMap.put(IMPORT_OPTIONS.ADDITIONAL_JDBC_PROPERTIES, additionalProperties);
	}
	
	public HashMap<String, Object> getExternalMetamodel() {
		return (HashMap<String, Object>) thisMap.get(IMPORT_OPTIONS.EXTERNAL_METAMODEL);
	}
	
	public void setExternalMetamodel(HashMap<String, Object> externalMetamodel) {
		thisMap.put(IMPORT_OPTIONS.EXTERNAL_METAMODEL, externalMetamodel);
	}
	
	/**
	 * Get the map that defines the value of objects specified in a prop file, but do not exist as a header
	 * @return
	 */
	public Map<String, String> getObjectValueMap() {
		return (Map<String, String>) thisMap.get(IMPORT_OPTIONS.OBJECT_VALUE_MAP);
	}
	
	public void setObjectValueMap(Map<String, String> objectValueMap) {
		thisMap.put(IMPORT_OPTIONS.OBJECT_VALUE_MAP, objectValueMap);
	}
	
	/**
	 * Get the map that defines the type of objects specified in a prop file, but do not exist as a header
	 * @return
	 */
	public Map<String, String> getObjectTypeMap() {
		return (Map<String, String>) thisMap.get(IMPORT_OPTIONS.OBJECT_TYPE_MAP);
	}
	
	public void setObjectTypeMap(Map<String, String> objectTypeMap) {
		thisMap.put(IMPORT_OPTIONS.OBJECT_TYPE_MAP, objectTypeMap);
	}
	
	/**
	 * Get the key that can be specified in a prop file to store the row number in a column
	 * @return
	 */
	public String getRowKey() {
		return (String) thisMap.get(IMPORT_OPTIONS.ROW_KEY);
	}
	
	public void setRowKey(String rowKey) {
		thisMap.put(IMPORT_OPTIONS.ROW_KEY, rowKey);
	}
	
	/**
	 * Get whether or not indexes should be created on the upload
	 * @return
	 */
	public boolean getCreateIndexes() {
		return (boolean) thisMap.get(IMPORT_OPTIONS.CREATE_INDEXES);
	}
	
	public void setCreateIndexes(boolean createIndexes) {
		thisMap.put(IMPORT_OPTIONS.CREATE_INDEXES, createIndexes);
	}
	
	/**
	 * TODO: only used by flat upload
	 * Get whether or not to clean string inputs before 
	 * @return
	 */
	public boolean getCleanString() {
		return (boolean) thisMap.get(IMPORT_OPTIONS.CLEAN_STRING);
	}
	
	public void setCleanString(boolean cleanString) {
		thisMap.put(IMPORT_OPTIONS.CLEAN_STRING, cleanString);
	}
	
	public void setEngineID(String engineID) {
		this.thisMap.put(IMPORT_OPTIONS.ENGINE_ID, engineID);
	}
	public String getEngineID() {
		return (String) this.thisMap.get(IMPORT_OPTIONS.ENGINE_ID);
	}
	
	/**
	 * Used to get the new data types map used for uploading data with reactors
	 * @param dataTypes
	 */
	public void setDataTypes(Map[] dataTypes) {
		this.thisMap.put(IMPORT_OPTIONS.CSV_DATATYPES, dataTypes);
	}
	public Map[] getDataTypes() {
		return (Map[]) this.thisMap.get(IMPORT_OPTIONS.CSV_DATATYPES);
	}
	
	///////////////////////////// end getters & setters /////////////////////////////////////

}
