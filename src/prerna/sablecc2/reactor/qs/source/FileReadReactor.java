package prerna.sablecc2.reactor.qs.source;

import java.util.Map;
import java.util.regex.Matcher;

import prerna.query.querystruct.AbstractFileQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class FileReadReactor extends AbstractQueryStructReactor {

	//keys to get inputs from pixel command
	private static final String FILEPATH = ReactorKeysEnum.FILE_PATH.getKey();
	private static final String SHEET_NAME = "sheetName";
	private static final String SHEET_RANGE = "sheetRange";
	private static final String DATA_TYPES = ReactorKeysEnum.DATA_TYPE_MAP.getKey();
	private static final String DELIMITER = ReactorKeysEnum.DELIMITER.getKey();
	private static final String HEADER_NAMES = ReactorKeysEnum.NEW_HEADER_NAMES.getKey();
	private static final String ADDITIONAL_DATA_TYPES = ReactorKeysEnum.ADDITIONAL_DATA_TYPES.getKey();

	/**
	 * FileRead args 
	 * 
	 * FILE=["filePath"]
	 * 
	 * for csv must add delim = ["delimiter"]
	 * for excel must add sheetName = ["sheetName"]
	 * 
	 * to set dataTypes 
	 *     dataTypesMap = [{"column", "type"}]
	 * to set newHeaders
	 *     newHeaders = [{"oldColumn", "newColumn"}] 
	 */
	
	public FileReadReactor() {
		this.keysToGet = new String[]{FILEPATH, SHEET_NAME, SHEET_RANGE, DATA_TYPES, DELIMITER, HEADER_NAMES, ADDITIONAL_DATA_TYPES};
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {
		AbstractFileQueryStruct qs = null;

		// get inputs
		Map<String, String> newHeaders = getHeaders(); 
		Map<String, String> dataTypes = getDataTypes();
		Map<String, String> additionalDataTypes = getAdditionalDataTypes();
		String fileLocation = getFileLocation();
		String fileExtension = "";

		// get file extension to determine qs
		boolean isExcel = false;
		if (fileLocation.contains(".")) {
			fileExtension = fileLocation.substring(fileLocation.indexOf('.'), fileLocation.length());
			if(fileExtension.equals(".xls") || fileExtension.equals(".xlsx") || fileExtension.equals(".xlsm")) {
				isExcel = true;
			}
		}
		if(isExcel) { // set excelQS
			// get excel inputs
			String sheetName = getSheetName();
			String sheetRange = getRange();
			qs = new ExcelQueryStruct();
			((ExcelQueryStruct) qs).setSheetName(sheetName);
			((ExcelQueryStruct) qs).setSheetRange(sheetRange);
		} else { // set csv qs
			char delimiter = getDelimiter();
			qs = new CsvQueryStruct();
			((CsvQueryStruct) qs).setDelimiter(delimiter);
		}
		// general inputs
		qs.setFilePath(fileLocation);
		qs.setNewHeaderNames(newHeaders);
		qs.setColumnTypes(dataTypes);
		qs.setAdditionalTypes(additionalDataTypes);
		qs.merge(this.qs);
		return qs;
	}

	/**************************************************************************************************
	 ************************************* INPUT METHODS***********************************************
	 **************************************************************************************************/

	private Map<String, String> getHeaders() {
		GenRowStruct headersGRS = this.store.getNoun(HEADER_NAMES);
		Map<String, String> headers = null;
		NounMetadata dataNoun;
		if (headersGRS != null) {
			dataNoun = headersGRS.getNoun(0);
			headers = (Map<String, String>) dataNoun.getValue();
		}
		return headers;
	}

	private String getSheetName() {
		GenRowStruct sheetGRS = this.store.getNoun(SHEET_NAME);
		String sheetName = "";
		NounMetadata sheetNoun;
		if (sheetGRS != null) {
			sheetNoun = sheetGRS.getNoun(0);
			sheetName = (String) sheetNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + SHEET_NAME + "=[sheetName] in pixel command");
		}
		return sheetName;
	}
	
	private String getRange() {
		GenRowStruct rangeGRS = this.store.getNoun(SHEET_RANGE);
		String sheetRange = "";
		NounMetadata rangeNoun;
		if (rangeGRS != null) {
			rangeNoun = rangeGRS.getNoun(0);
			sheetRange = (String) rangeNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + SHEET_RANGE + "=[sheetRange] in pixel command");
		}
		return sheetRange;
	}

	private Map<String, String> getDataTypes() {
		GenRowStruct dataTypeGRS = this.store.getNoun(DATA_TYPES);
		Map<String, String> dataTypes = null;
		NounMetadata dataNoun;
		if (dataTypeGRS != null) {
			dataNoun = dataTypeGRS.getNoun(0);
			dataTypes = (Map<String, String>) dataNoun.getValue();
		}
		return dataTypes;
	}
	
	private Map<String, String> getAdditionalDataTypes() {
		GenRowStruct dataTypeGRS = this.store.getNoun(ADDITIONAL_DATA_TYPES);
		Map<String, String> dataTypes = null;
		NounMetadata dataNoun;
		if (dataTypeGRS != null) {
			dataNoun = dataTypeGRS.getNoun(0);
			dataTypes = (Map<String, String>) dataNoun.getValue();
		}
		return dataTypes;
	}

	private char getDelimiter() {
		GenRowStruct delimGRS = this.store.getNoun(DELIMITER);
		String delimiter = "";
		char delim = ','; //default
		NounMetadata instanceIndexNoun;

		if (delimGRS != null) {
			instanceIndexNoun = delimGRS.getNoun(0);
			delimiter = (String) instanceIndexNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + DELIMITER + "=[delimiter] in pixel command");
		}

		//get char from input string
		if(delimiter.length() > 0) {
			delim = delimiter.charAt(0);
		}

		return delim;
	}

	private String getFileLocation() {
		GenRowStruct fileGRS = this.store.getNoun(FILEPATH);
		String fileLocation = "";
		NounMetadata fileNoun;
		if (fileGRS != null) {
			fileNoun = fileGRS.getNoun(0);
			fileLocation = (String) fileNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + FILEPATH + "=[filePath] in pixel command");
		}
		
		// we will need to translate the file path from the parameterized insight form
		if(fileLocation.startsWith("$IF")) {
			fileLocation = fileLocation.replaceFirst("\\$IF", Matcher.quoteReplacement(this.insight.getInsightFolder()));
		}
		
		return fileLocation;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SHEET_NAME)) {
			return "The excel sheet name";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}