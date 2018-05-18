package prerna.sablecc2.reactor.app.upload;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

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
