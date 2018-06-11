package prerna.sablecc2.reactor.app.upload;

import java.io.File;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public abstract class AbstractRdbmsUploadReactor extends AbstractReactor {

	protected final String APP = ReactorKeysEnum.APP.getKey();
	protected final String FILE_PATH = ReactorKeysEnum.FILE_PATH.getKey();
	protected final String ADD_TO_EXISTING = "existing";
	protected final String CLEAN_STRING_VALUES = "clean";
	protected final String REMOVE_DUPLICATE_ROWS = "deduplicate";

	// only applies for "csv" uploading - doesn't need to be ","
	protected final String DELIMITER = ReactorKeysEnum.DELIMITER.getKey();

	// these will have different formats if it is a 
	// text-based file vs. if it is an excel file
	protected final String DATA_TYPE_MAP = ReactorKeysEnum.DATA_TYPE_MAP.getKey();
	protected final String NEW_HEADERS = ReactorKeysEnum.NEW_HEADER_NAMES.getKey();
	protected final String ADDITIONAL_TYPES = "additionalTypes";

	///////////////////////////////////////////////////////

	/*
	 * Execution methods
	 */

	public abstract String generateNewApp(String appName, String filePath, Logger logger);

	public abstract String addToExistingApp(String appName, String filePath, Logger logger);

	/*
	 * Methods that are shared
	 */

	/**
	 * Update local master
	 * @param appId
	 */
	protected void updateLocalMaster(String appId) {
		Utility.synchronizeEngineMetadata(appId);
	}

	/**
	 * Update solr
	 * @param appId
	 * @throws Exception
	 */
	protected void updateSolr(String appId) throws Exception {
		SolrUtility.addToSolrInsightCore(appId);
		SolrUtility.addAppToSolr(appId);
	}

	/**
	 * Add the metadata into the OWL
	 * @param owler
	 * @param tableName
	 * @param uniqueRowId
	 * @param headers
	 * @param sqlTypes
	 */
	protected void generateTableMetadata(OWLER owler, String tableName, String uniqueRowId, String[] headers, String[] sqlTypes, String[] additionalTypes) {
		// add the main column
		owler.addConcept(tableName, uniqueRowId, OWLER.BASE_URI, "LONG");
		// add the props
		for(int i = 0; i < headers.length; i++) {
			// NOTE ::: SQL_TYPES will have the added unique row id at index 0
			owler.addProp(tableName, uniqueRowId, headers[i], sqlTypes[i+1], additionalTypes[i]);
		}
	}

	/**
	 * Used to update DIHelper
	 * Should only be used when making new app
	 * @param newAppName
	 * @param engine
	 * @param smssFile
	 */
	protected void updateDIHelper(String newAppId, String newAppName, IEngine engine, File smssFile) {
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		DIHelper.getInstance().setLocalProperty(newAppId, engine);
		String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		engineNames = engineNames + ";" + newAppId;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
	}

	/**
	 * Create a new table given the table name, headers, and types
	 * Returns the sql types that were generated
	 * @param tableName
	 * @param headers
	 * @param types
	 */
	protected String[] createNewTable(IEngine engine, String tableName, String uniqueRowId, String[] headers, SemossDataType[] types) {
		// we need to add the identity column
		int size = types.length;
		String[] sqlTypes = new String[size+1];
		String[] newHeaders = new String[size+1];

		newHeaders[0] = uniqueRowId;
		sqlTypes[0] = "IDENTITY";
		for(int i = 0; i < size; i++) {
			newHeaders[i+1] = headers[i];
			SemossDataType sType = types[i];
			if(sType == SemossDataType.STRING || sType == SemossDataType.FACTOR) {
				sqlTypes[i+1] = "VARCHAR(2000)";
			} else if(sType == SemossDataType.INT) {
				sqlTypes[i+1] = "INT";
			} else if(sType == SemossDataType.DOUBLE) {
				sqlTypes[i+1] = "DOUBLE";
			} else if(sType == SemossDataType.DATE) {
				sqlTypes[i+1] = "DATE";
			} else if(sType == SemossDataType.TIMESTAMP) {
				sqlTypes[i+1] = "TIMESTAMP ";
			}
		}

		String createTable = RdbmsQueryBuilder.makeOptionalCreate(tableName, newHeaders, sqlTypes);
		engine.insertData(createTable);
		return sqlTypes;
	}

	/**
	 * 
	 * @param engine
	 * @param tableName
	 * @param columnName
	 */
	protected void addIndex(IEngine engine, String tableName, String columnName) {
		String indexName = columnName.toUpperCase() + "_INDEX" ;
		String indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + columnName.toUpperCase() + ")";
		engine.insertData(indexSql);
	}
	
	///////////////////////////////////////////////////////

	/*
	 * Getters from noun store
	 */

	protected String getAppName() {
		GenRowStruct grs = this.store.getNoun(APP);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the new app name using key " + APP);
		}
		return grs.get(0).toString();
	}

	protected String getFilePath() {
		GenRowStruct grs = this.store.getNoun(FILE_PATH);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the file path using key " + FILE_PATH);
		}
		return grs.get(0).toString();
	}

	protected boolean getExisting() {
		GenRowStruct grs = this.store.getNoun(ADD_TO_EXISTING);
		if(grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}

	protected boolean getClean() {
		GenRowStruct grs = this.store.getNoun(CLEAN_STRING_VALUES);
		if(grs == null || grs.isEmpty()) {
			return true;
		}
		return (boolean) grs.get(0);
	}

	protected boolean getDeduplicateRows() {
		GenRowStruct grs = this.store.getNoun(REMOVE_DUPLICATE_ROWS);
		if(grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}

	protected String getDelimiter() {
		GenRowStruct grs = this.store.getNoun(DELIMITER);
		if(grs == null || grs.isEmpty()) {
			return ",";
		}
		return grs.get(0).toString();
	}

}
