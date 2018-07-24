package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class UploadInputUtility {
	public static final String APP = ReactorKeysEnum.APP.getKey();
	public static final String FILE_PATH = ReactorKeysEnum.FILE_PATH.getKey();
	public static final String DATA_TYPE_MAP = ReactorKeysEnum.DATA_TYPE_MAP.getKey();
	public static final String ADDITIONAL_DATA_TYPES = ReactorKeysEnum.ADDITIONAL_DATA_TYPES.getKey();
	public static final String NEW_HEADERS = ReactorKeysEnum.NEW_HEADER_NAMES.getKey();
	public static final String ADD_TO_EXISTING = ReactorKeysEnum.EXISTING.getKey();
	public static final String CLEAN_STRING_VALUES = ReactorKeysEnum.CLEAN.getKey();
	public static final String REMOVE_DUPLICATE_ROWS = ReactorKeysEnum.DEDUPLICATE.getKey();
	public static final String METAMODEL = ReactorKeysEnum.METAMODEL.getKey();
	public static final String END_ROW = ReactorKeysEnum.END_ROW.getKey();
	public static final String START_ROW = ReactorKeysEnum.START_ROW.getKey();
	public static final String PROP_FILE = "propFile";
	public static final String CUSTOM_BASE_URI = "customBaseURI";
	public static final String CREATE_INDEX = ReactorKeysEnum.CREATE_INDEX.getKey();
	public static final String ROW_COUNT = ReactorKeysEnum.ROW_COUNT.getKey(); 

	// defaults
	public static final int START_ROW_INT = 2;
	public static final int END_ROW_INT = 2_000_000_000;
	public static final String SEMOSS_URI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);

	// only applies for "csv" uploading - doesn't need to be ","
	public static final String DELIMITER = ReactorKeysEnum.DELIMITER.getKey();


	public static String getAppName(NounStore store) {
		GenRowStruct grs = store.getNoun(APP);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the new app name using key " + APP);
		}
		return grs.get(0).toString();
	}

	public static String getFilePath(NounStore store) {
		GenRowStruct grs = store.getNoun(FILE_PATH);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define the file path using key " + FILE_PATH);
		}
		return grs.get(0).toString();
	}

	public static boolean getExisting(NounStore store) {
		GenRowStruct grs = store.getNoun(ADD_TO_EXISTING);
		if (grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}

	public static boolean getClean(NounStore store) {
		GenRowStruct grs = store.getNoun(CLEAN_STRING_VALUES);
		if (grs == null || grs.isEmpty()) {
			return true;
		}
		return (boolean) grs.get(0);
	}

	public static boolean getDeduplicateRows(NounStore store) {
		GenRowStruct grs = store.getNoun(REMOVE_DUPLICATE_ROWS);
		if (grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}
	
	public static String getCustomBaseURI(NounStore store) {
		GenRowStruct grs = store.getNoun(CUSTOM_BASE_URI);
		if (grs == null || grs.isEmpty()) {
			return SEMOSS_URI;
		}
		return grs.get(0).toString();
	}



	//////////////////////////////////////////////////////////
	// CSV methods
	//////////////////////////////////////////////////////////
	
	public static String getDelimiter(NounStore store) {
		GenRowStruct grs = store.getNoun(DELIMITER);
		if (grs == null || grs.isEmpty()) {
			return ",";
		}
		return grs.get(0).toString();
	}
	
	public static Map<String, String> getAdditionalCsvDataTypes(NounStore store) {
		GenRowStruct grs = store.getNoun(ADDITIONAL_DATA_TYPES);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	public static Map<String, String> getNewCsvHeaders(NounStore store) {
		GenRowStruct grs = store.getNoun(NEW_HEADERS);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	protected static Map<String, String> getCsvDataTypeMap(NounStore store) {
		GenRowStruct grs = store.getNoun(DATA_TYPE_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	/**
	 * Figure out the end row count from the csv file
	 * 
	 * @return
	 */
	public static boolean getRowCount(NounStore store) {
		GenRowStruct boolGrs = store.getNoun(ROW_COUNT);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

	//////////////////////////////////////////////////////////
	// Metamodel methods
	//////////////////////////////////////////////////////////

	/**
	 * Standardize metamodelProperties from pixel inputs or prop file
	 * 
	 * @param store
	 * @return
	 */
	public static Map<String, Object> getMetamodelProps(NounStore store) {
		// get metamodel from pixel input or prop file
		Map<String, Object> metamodel = UploadInputUtility.getMetamodel(store);
		Map<String, String> dataTypesMap = null;
		if (metamodel == null) {
			metamodel = UploadInputUtility.getMetamodelFromPropFile(store);
			dataTypesMap = (Map<String, String>) metamodel.get(Constants.DATA_TYPES);
		} else {
			// if we get the metamodel from the pixel input
			// add datatypes
			dataTypesMap = UploadInputUtility.getCsvDataTypeMap(store);
			metamodel.put(Constants.DATA_TYPES, dataTypesMap);
			// add start row
			int startRow = UploadInputUtility.getStartRow(store);
			metamodel.put(Constants.START_ROW, startRow);
			// add end row
			Integer endRow = UploadInputUtility.getEndRow(store);
			if (endRow != null) {
				metamodel.put(Constants.END_ROW, endRow);
			}
		}
		return metamodel;
	}

	private static Map<String, Object> getMetamodel(NounStore store) {
		GenRowStruct grs = store.getNoun(METAMODEL);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, Object>) grs.get(0);
	}

	public static Map<String, Object> getMetamodelFromPropFile(NounStore store) {
		GenRowStruct grs = store.getNoun(PROP_FILE);
		if (!(grs == null || grs.isEmpty())) {
			// TODO next try reading from prop file
			String metamodelPath = grs.get(0).toString();
			try {
				Map<String, Object> result = new ObjectMapper().readValue(new File(metamodelPath), Map.class);
				return result;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private static int getStartRow(NounStore store) {
		GenRowStruct grs = store.getNoun(START_ROW);
		if (grs == null || grs.isEmpty()) {
			return START_ROW_INT;
		}
		return (int) grs.get(0);
	}

	private static Integer getEndRow(NounStore store) {
		GenRowStruct grs = store.getNoun(START_ROW);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (int) grs.get(0);
	}


}
