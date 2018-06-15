package prerna.sablecc2.reactor.app.upload;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

public class UploadInputUtility {
	public static final String APP = ReactorKeysEnum.APP.getKey();
	public static final String FILE_PATH = ReactorKeysEnum.FILE_PATH.getKey();
	public static final String ADD_TO_EXISTING = ReactorKeysEnum.EXISTING.getKey();
	public static final String CLEAN_STRING_VALUES = ReactorKeysEnum.CLEAN.getKey();
	public static final String REMOVE_DUPLICATE_ROWS = ReactorKeysEnum.DEDUPLICATE.getKey();
	
	// only applies for "csv" uploading - doesn't need to be ","
	public static final String DELIMITER = ReactorKeysEnum.DELIMITER.getKey();

	/*
	 * Getters from noun store
	 */

	public static String getAppName(NounStore store) {
		GenRowStruct grs = store.getNoun(APP);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the new app name using key " + APP);
		}
		return grs.get(0).toString();
	}

	public static String getFilePath(NounStore store) {
		GenRowStruct grs = store.getNoun(FILE_PATH);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the file path using key " + FILE_PATH);
		}
		return grs.get(0).toString();
	}

	public static boolean getExisting(NounStore store) {
		GenRowStruct grs = store.getNoun(ADD_TO_EXISTING);
		if(grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}

	public static boolean getClean(NounStore store) {
		GenRowStruct grs = store.getNoun(CLEAN_STRING_VALUES);
		if(grs == null || grs.isEmpty()) {
			return true;
		}
		return (boolean) grs.get(0);
	}

	public static boolean getDeduplicateRows(NounStore store) {
		GenRowStruct grs = store.getNoun(REMOVE_DUPLICATE_ROWS);
		if(grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}

	public static String getDelimiter(NounStore store) {
		GenRowStruct grs = store.getNoun(DELIMITER);
		if(grs == null || grs.isEmpty()) {
			return ",";
		}
		return grs.get(0).toString();
	}
	
	
}
