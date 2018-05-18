package prerna.sablecc2.reactor.app.upload;

import java.io.File;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
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

	public abstract void generateNewApp(String appName, String filePath, Logger logger);
	
	public abstract void addToExistingApp(String appName, String filePath, Logger logger);
	
	/*
	 * Methods that are shared
	 */
	
	/**
	 * Update local master
	 * @param appName
	 */
	protected void updateLocalMaster(String appName) {
		Utility.synchronizeEngineMetadata(appName);
	}
	
	/**
	 * Update solr
	 * @param appName
	 * @throws Exception
	 */
	protected void updateSolr(String appName) throws Exception {
		SolrUtility.addToSolrInsightCore(appName);
		SolrUtility.addAppToSolr(appName);
	}
	
	/**
	 * Used to update DIHelper
	 * Should only be used when making new app
	 * @param newAppName
	 * @param engine
	 * @param smssFile
	 */
	protected void updateDIHelper(String newAppName, IEngine engine, File smssFile) {
		DIHelper.getInstance().getCoreProp().setProperty(newAppName + "_" + Constants.STORE, smssFile.getAbsolutePath());
		DIHelper.getInstance().setLocalProperty(newAppName, engine);
		String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		engineNames = engineNames + ";" + newAppName;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
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
