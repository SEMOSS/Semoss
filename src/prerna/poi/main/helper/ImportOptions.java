package prerna.poi.main.helper;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.util.sql.SQLQueryUtil;

public class ImportOptions {

	/**
	 * This class is set as a wrapper such that you do not need to know the specific keys for each of these import options
	 * but can grab and set the values you want
	 */
	
	private Hashtable<ImportOptions.IMPORT_OPTIONS , Object> thisMap = new Hashtable<ImportOptions.IMPORT_OPTIONS , Object>();
	
	public enum IMPORT_METHOD {CREATE_NEW, ADD_TO_EXISTING, OVERRIDE, CONNECT_TO_EXISTING_RDBMS};
	public enum IMPORT_TYPE {CSV, NLP, EXCEL_POI, EXCEL, OCR, CSV_FLAT_LOAD};
	public enum DB_TYPE {RDF, RDBMS};
	
	public enum IMPORT_OPTIONS {
		SMSS_LOCATION, 			// should point to string - location of smss file
		IMPORT_METHOD, 			// should point to IMPORT_METHOD - either create new, add to existing, override
		IMPORT_TYPE, 			// should point to ImportDataProcessor.IMPORT_TYPE - csv, nlp, etc.
		DB_TYPE, 				// should point to ImportDataProcess.DB_TYPE - rdf or rdbms
		FILE_LOCATION, 			// should point to string - the file paths as a single string which are ";" delimited
		BASE_URL, 				// should point to string - the base URI for the db (http://health.mil/ontologies for MHS)
		DB_NAME, 				// should point to string - either the name of the new DB to create or the existing DB to alter
		BASE_FOLDER,			// should point to string - the base workspace location on the machine
		QUESTION_FILE, 			// should point to string - the file path to the question file
		DB_DRIVER_TYPE, 		// should point to SQLQueryUtil.DB_TYPE - only valid for RDBMS where we need to specify the type
		ALLOW_DUPLICATES, 		// should point to boolean - only valid for RDBMS where we should remove duplicate values in tables
		AUTO_LOAD, 				// should point to boolean - determine if we should load the database directly or shut it down and have
								// have the smss watcher find the engine and load it
		DEFINED_METAMODEL,		// should point to Hashtable<String, String>[] - only valid for csv upload
		DATA_TYPE_MAP			// should point to Map<String, Map<String, String>> - only valid for flat upload
	};
	
	
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
	
	public String getBaseFolder() {
		return (String) thisMap.get(IMPORT_OPTIONS.BASE_FOLDER);
	}
	
	public void setBaseFolder(String baseFolder) {
		thisMap.put(IMPORT_OPTIONS.BASE_FOLDER, baseFolder);
	}
	
	/**
	 * Get the question file location
	 * @return
	 */
	public String getQuestionFile() {
		return (String) thisMap.get(IMPORT_OPTIONS.QUESTION_FILE);
	}
	
	public void setQuestionFile(String questionFileName) {
		thisMap.put(IMPORT_OPTIONS.QUESTION_FILE, questionFileName);
	}
	
	/**
	 * Get the specific type of RDBMS engine
	 * @return
	 */
	public SQLQueryUtil.DB_TYPE getRDBMSDriverType() {
		return (SQLQueryUtil.DB_TYPE) thisMap.get(IMPORT_OPTIONS.DB_DRIVER_TYPE);
	}
	
	public void setRDBMSDriverType(SQLQueryUtil.DB_TYPE rdbmsDriverType) {
		thisMap.put(IMPORT_OPTIONS.DB_DRIVER_TYPE, rdbmsDriverType);
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
	 * Set the metamodel for a set of csv files
	 * @return
	 */
	public Hashtable<String, String>[] getMetamodelArray() {
		return (Hashtable<String, String>[]) thisMap.get(IMPORT_OPTIONS.DEFINED_METAMODEL);
	}
	
	public void setMetamodelArray(Hashtable<String, String>[] metamodelArray) {
		thisMap.put(IMPORT_OPTIONS.DEFINED_METAMODEL, metamodelArray);
	}
	
	/**
	 * Set the dataTypeMap for a set of files
	 * @return
	 */
	public List<Map<String, String[]>> getDataTypeMap() {
		return (List<Map<String, String[]>>) thisMap.get(IMPORT_OPTIONS.DATA_TYPE_MAP);
	}
	
	public void setDataTypeMap(List<Map<String, String[]>> dataTypeMap) {
		thisMap.put(IMPORT_OPTIONS.DATA_TYPE_MAP, dataTypeMap);
	}
	
	///////////////////////////// end getters & setters /////////////////////////////////////

}
