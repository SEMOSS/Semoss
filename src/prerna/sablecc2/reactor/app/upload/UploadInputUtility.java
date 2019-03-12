package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UploadInputUtility {

	public static final String APP = ReactorKeysEnum.APP.getKey();
	public static final String FILE_PATH = ReactorKeysEnum.FILE_PATH.getKey();
	public static final String ADD_TO_EXISTING = ReactorKeysEnum.EXISTING.getKey();
	public static final String CLEAN_STRING_VALUES = ReactorKeysEnum.CLEAN.getKey();
	public static final String REMOVE_DUPLICATE_ROWS = ReactorKeysEnum.DEDUPLICATE.getKey();
	public static final String REPLACE_EXISTING = ReactorKeysEnum.REPLACE.getKey();
	public static final String METAMODEL = ReactorKeysEnum.METAMODEL.getKey();
	public static final String END_ROW = ReactorKeysEnum.END_ROW.getKey();
	public static final String START_ROW = ReactorKeysEnum.START_ROW.getKey();
	public static final String PROP_FILE = "propFile";
	public static final String CUSTOM_BASE_URI = "customBaseURI";
	public static final String CREATE_INDEX = ReactorKeysEnum.CREATE_INDEX.getKey();
	public static final String ROW_COUNT = ReactorKeysEnum.ROW_COUNT.getKey();
	// these will have different formats if it is a
	// text-based file vs. if it is an excel file
	public static final String DATA_TYPE_MAP = ReactorKeysEnum.DATA_TYPE_MAP.getKey();
	public static final String ADDITIONAL_DATA_TYPES = ReactorKeysEnum.ADDITIONAL_DATA_TYPES.getKey();
	public static final String NEW_HEADERS = ReactorKeysEnum.NEW_HEADER_NAMES.getKey();
	
	// additional metadata fields on OWL
	public static final String DESCRIPTION_MAP = "descriptionMap";
	public static final String LOGICAL_NAMES_MAP = "logicalNamesMap";
	
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
		
		NounMetadata noun = grs.getNoun(0);
		if(noun.getNounType() == PixelDataType.UPLOAD_RETURN_MAP) {
			Map<String, Object> uploadMap = (Map<String, Object>) noun.getValue();
			return uploadMap.get("app_id").toString();
		}
		return noun.getValue().toString();
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
	
	public static boolean getReplace(NounStore store) {
		GenRowStruct grs = store.getNoun(REPLACE_EXISTING);
		if (grs == null || grs.isEmpty()) {
			return false;
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

	public static Map<String, String> getCsvDataTypeMap(NounStore store) {
		GenRowStruct grs = store.getNoun(DATA_TYPE_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}

	public static Map<String, String> getCsvDescriptions(NounStore store) {
		GenRowStruct grs = store.getNoun(DESCRIPTION_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	public static Map<String, List<String>> getCsvLogicalNames(NounStore store) {
		GenRowStruct grs = store.getNoun(LOGICAL_NAMES_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, List<String>>) grs.get(0);
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
	
	public Map<String, String> getDescriptionMap(NounStore store) {
		GenRowStruct grs = store.getNoun(DESCRIPTION_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) grs.get(0);
	}
	
	public Map<String, List<String>> getLogicalNamesMap(NounStore store) {
		GenRowStruct grs = store.getNoun(LOGICAL_NAMES_MAP);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		return (Map<String, List<String>>) grs.get(0);
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
			// add new headers
			Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(store);
			metamodel.put(NEW_HEADERS, newHeaders);
			// add additionalDataTypes
			Map<String, String> additionalDataTypes = UploadInputUtility.getAdditionalCsvDataTypes(store);
			metamodel.put(ADDITIONAL_DATA_TYPES, additionalDataTypes);
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
			String metamodelPath = grs.get(0).toString();
			if (metamodelPath.toLowerCase().endsWith(".prop")) {
				// using old prop file need to convert
				return convertPropFile(metamodelPath, store);
			}
			try {
				// using new json prop file
				Map<String, Object> result = new ObjectMapper().readValue(new File(metamodelPath), Map.class);
				return result;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private static Map<String, Object> convertPropFile(String oldMetamodelPath, NounStore store) {
		// need to convert old prop file to json
		Properties oldMetamodel = Utility.loadProperties(oldMetamodelPath);
		HashMap<String, Object> newMetamodel = new HashMap<>();
		// get node properties
		String nodePropStr = (String) oldMetamodel.get("NODE_PROP");
		HashMap<String, List<String>> nodePropMap = new HashMap<>();
		if (nodePropStr.contains(";")) {
			String[] nodeProps = nodePropStr.split(";");
			for (String nodeStr : nodeProps) {
				String[] propSplit = nodeStr.split("%");
				String node = propSplit[0];
				String prop = propSplit[1];
				List<String> properties = new ArrayList<>();
				if (nodePropMap.containsKey(node)) {
					properties = nodePropMap.get(node);
				}
				properties.add(prop);
				nodePropMap.put(node, properties);
			}
		}
		newMetamodel.put(Constants.NODE_PROP, nodePropMap);
		// get relations
		String relationStr = (String) oldMetamodel.get("RELATION");
		String[] relations = relationStr.split(";");
		ArrayList<Map<String, String>> relationships = new ArrayList<>();
		for (String relStr : relations) {
			if (relStr.contains("@")) {
				String[] rel = relStr.split("@");
				HashMap<String, String> relMap = new HashMap<>();
				String fromTable = rel[0];
				String toTable = rel[2];
				// check if tables are defined in node props if not add them
				// with no properties
				if (!nodePropMap.containsKey(fromTable)) {
					nodePropMap.put(fromTable, new ArrayList<String>());
				}
				if (!nodePropMap.containsKey(toTable)) {
					nodePropMap.put(toTable, new ArrayList<String>());
				}
				relMap.put(Constants.FROM_TABLE, fromTable);
				relMap.put(Constants.REL_NAME, rel[1]);
				relMap.put(Constants.TO_TABLE, toTable);
				relationships.add(relMap);
			}
		}
		newMetamodel.put(Constants.RELATION, relationships);
		// add start row, end row
		if (oldMetamodel.containsKey("START_ROW")) {
			newMetamodel.put(Constants.START_ROW, oldMetamodel.getProperty("START_ROW"));
		}
		if (oldMetamodel.containsKey("END_ROW")) {
			newMetamodel.put(Constants.END_ROW, oldMetamodel.getProperty("END_ROW"));
		}
		// ugh getting datatypes is not fun need to look at header index
		String csvFilePath = UploadInputUtility.getFilePath(store);
		String delimiter = UploadInputUtility.getDelimiter(store);
		char delim = delimiter.charAt(0);
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(csvFilePath);
		String[] headers = helper.getHeaders();
		Map<String, Object> dataTypes = new HashMap<>();
		for (int i = 0; i < headers.length; i++) {
			// headers are one off
			String columnHeaderIndex = i + 1 + "";
			//TODO hmmmm I need to get new header name and index
			if (oldMetamodel.containsKey(columnHeaderIndex)) {
				dataTypes.put(headers[i], oldMetamodel.get(columnHeaderIndex));
			}
		}
		helper.clear();
		newMetamodel.put(Constants.DATA_TYPES, dataTypes);
		return newMetamodel;
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
