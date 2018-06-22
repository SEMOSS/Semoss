package prerna.sablecc2.reactor.app.upload;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ArrayUtilityMethods;

public class ParseMetamodelReactor extends AbstractReactor {
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public ParseMetamodelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.ROW_COUNT.getKey(), ReactorKeysEnum.PROP_FILE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String csvFilePath = this.keyValue.get(this.keysToGet[0]);
		if (csvFilePath == null) {
			NounMetadata noun = new NounMetadata("Need to define " + this.keysToGet[0], PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String delimiter = this.keyValue.get(this.keysToGet[1]);
		if (delimiter == null) {
			delimiter = ",";
		}
		String propFilePath = this.keyValue.get(this.keysToGet[3]);
		if (propFilePath == null) {
			NounMetadata noun = new NounMetadata("Need to define " + this.keysToGet[4], PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		char delim = delimiter.charAt(0);
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(csvFilePath);
		return new NounMetadata(generateMetaModelFromProp(helper, propFilePath), PixelDataType.MAP);
	}

	/**
	 * Generates the Meta model data based on the definition of the prop file
	 */
	private Map<String, Object> generateMetaModelFromProp(CSVFileHelper helper, String propFile) {
		Properties propMap = null;
		InputStream input = null;
		try {
			input = new FileInputStream(propFile);
			propMap = new Properties();
			propMap.load(input);
		} catch (IOException e) {
			e.printStackTrace();
			String errorMsg = "Unable to read the .prop file";
			NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		String[] columnHeaders = helper.getHeaders();
		int size = columnHeaders.length;

		Map<String, SemossDataType> dataTypeMap = new LinkedHashMap<String, SemossDataType>();

		// use the prop file to get the data types
		for (int colIdx = 0; colIdx < size; colIdx++) {
			String dataType = propMap.getProperty((colIdx + 1) + "");
			if (dataType == null) {
				dataType = "STRING";
			}
			dataTypeMap.put(columnHeaders[colIdx], SemossDataType.convertStringToDataType(dataType));
		}
		Hashtable<String, String> additionalInfo = new Hashtable<String, String>();

		// if the mode is not set
		// it means we are dealing with the new flat table
		List<String[]> data = new ArrayList<>(500);
		String[] cells = null;
		int count = 1;
		int limit = 500;
		boolean getEndRowCount = getRowCount();
		while ((cells = helper.getNextRow()) != null) {
			if (count <= limit) {
				data.add(cells);
				count++;
			} else {
				// if we need to get total number of rows from csv continue
				if (getEndRowCount) {
					count++;
				} else {
					break;
				}
			}
		}
		int endRow = count;
		int startRow = 2;

		Map<String, List<Map<String, Object>>> propFileData = new HashMap<>();

		List<Map<String, Object>> relList = new ArrayList<>();
		List<Map<String, Object>> nodePropList = new ArrayList<>();
		propFileData.put("propFileRel", relList);
		propFileData.put("propFileNodeProp", nodePropList);

		// loop through everything in the prop file
		// if it is a special key, we will do some processing
		// otherwise, we just add it as it
		for (Object propKey : propMap.keySet()) {
			String propKeyS = propKey.toString().trim();

			// Parses text written as:
			// RELATION
			// Title@BelongsTo@Genre;Title@DirectedBy@Director;Title@DirectedAt@Studio;
			if (propKeyS.equals("RELATION")) {
				String relationText = propMap.getProperty(propKeyS).trim();
				if (!relationText.isEmpty()) {

					String[] relations = relationText.split(";");

					for (String relation : relations) {

						String[] components = relation.split("@");
						String subject = components[0].trim();
						String object = components[2].trim();

						// do some header checks
						if (subject.contains("+")) {
							String[] subSplit = subject.split("\\+");
							for (String sub : subSplit) {
								if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, sub)) {
									String errorMsg = "CSV does not contain header : " + sub + ".  Please update RELATION in .prop file";
									NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
									SemossPixelException exception = new SemossPixelException(noun);
									exception.setContinueThreadOfExecution(false);
									throw exception;		
								}
							}
						} else {
							if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
								String errorMsg = "CSV does not contain header : " + subject + ".  Please update RELATION in .prop file";
								NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
								SemossPixelException exception = new SemossPixelException(noun);
								exception.setContinueThreadOfExecution(false);
								throw exception;	
							}
						}

						if (object.contains("+")) {
							String[] objSplit = object.split("\\+");
							for (String obj : objSplit) {
								if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, obj)) {
									String errorMsg = "CSV does not contain header : " + obj + ".  Please update RELATION in .prop file";
									NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
									SemossPixelException exception = new SemossPixelException(noun);
									exception.setContinueThreadOfExecution(false);
									throw exception;	
								}
							}
						} else {
							if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
								String errorMsg = "CSV does not contain header : " + object + ".  Please update RELATION in .prop file";
								NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
								SemossPixelException exception = new SemossPixelException(noun);
								exception.setContinueThreadOfExecution(false);
								throw exception;
							}
						}

						String[] subjectArr = { subject };
						String predicate = components[1];
						String[] objectArr = { object };

						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("pred", predicate);
						predMap.put("obj", objectArr);
						propFileData.get("propFileRel").add(predMap);
					}
				}
			}

			// Parses text written as :
			// NODE_PROP
			// Title%RevenueDomestic;Title%RevenueInternational;Title%MovieBudget;Title%RottenTomatoesCritics;Title%RottenTomatoesAudience;Title%Nominated;
			else if (propKeyS.equals("NODE_PROP")) {
				String nodePropText = propMap.getProperty(propKeyS).trim();
				if (!nodePropText.isEmpty()) {
					String[] nodeProps = nodePropText.split(";");

					for (String nodeProp : nodeProps) {

						String[] components = nodeProp.split("%");

						String subject = components[0].trim();
						String object = components[1].trim();

						// do some header checks
						if (subject.contains("+")) {
							String[] subSplit = subject.split("\\+");
							for (String sub : subSplit) {
								if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, sub)) {
									String errorMsg = "CSV does not contain header : " + sub + ".  Please update RELATION in .prop file";
									NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
									SemossPixelException exception = new SemossPixelException(noun);
									exception.setContinueThreadOfExecution(false);
									throw exception;
								}
							}
						} else {
							if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, subject)) {
								String errorMsg = "CSV does not contain header : " + subject + ".  Please update RELATION in .prop file";
								NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
								SemossPixelException exception = new SemossPixelException(noun);
								exception.setContinueThreadOfExecution(false);
								throw exception;
							}
						}

						if (object.contains("+")) {
							String[] objSplit = object.split("\\+");
							for (String obj : objSplit) {
								if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, obj)) {
									String errorMsg = "CSV does not contain header : " + obj + ".  Please update RELATION in .prop file";
									NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
									SemossPixelException exception = new SemossPixelException(noun);
									exception.setContinueThreadOfExecution(false);
									throw exception;
								}
							}
						} else {
							if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, object)) {
								String errorMsg = "CSV does not contain header : " + object + ".  Please update RELATION in .prop file";
								NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
								SemossPixelException exception = new SemossPixelException(noun);
								exception.setContinueThreadOfExecution(false);
								throw exception;
							}
						}

						String[] subjectArr = { subject };
						String[] objectArr = { object };

						Map<String, Object> predMap = new HashMap<>();
						predMap.put("sub", subjectArr);
						predMap.put("prop", objectArr);

						SemossDataType dataType = dataTypeMap.get(object);
						predMap.put("dataType", dataType);
						propFileData.get("propFileNodeProp").add(predMap);
					}
				}
			} else if (propKeyS.equals("START_ROW")) {
				String startRowStr = propMap.getProperty(propKeyS);
				try {
					startRow = Integer.parseInt(startRowStr);
				} catch (NumberFormatException e) {
				}
			}

			else if (propKeyS.equals("END_ROW")) {
				String endRowStr = propMap.getProperty(propKeyS);
				try {
					endRow = Integer.parseInt(endRowStr);
				} catch (NumberFormatException e) {

				}
			}

			else if (propKeyS.equals("RELATION_PROP")) {
				// do nothing, we don't do anything with relationship props at
				// the moment
			}

			else {
				// WE WANT TO IGNORE SOME THINGS THAT WE DO NOT COUNT AS
				// ADDITIONAL PROPERTIES
				// THIS IS BECAUSE WE WRITE THE DATA TYPES IN THE PROP FILE
				// 1) if the key is NUM_COLUMNS
				if (propKeyS.equals("NUM_COLUMNS")) {
					continue;
				}
				// 2) if the value is STRING, NUMBER, or DATE
				String value = propMap.getProperty(propKeyS);
				if (value.equals("STRING") || value.equals("NUMBER") || value.equals("DATE")) {
					continue;
				}
				additionalInfo.put(propKeyS, propMap.getProperty(propKeyS));
			}
		}
		Map<String, Object> fileMetaModelData = new HashMap<String, Object>();
		fileMetaModelData.putAll(propFileData);
		// get file location and file name
		String filePath = helper.getFileLocation();
		String file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
				filePath.lastIndexOf("."));
		try {
			file = file.substring(0, file.indexOf("_____UNIQUE"));
		} catch (Exception e) {
			// just in case that fails, this shouldnt because if its a filename
			// it should have a "."
			file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
					filePath.lastIndexOf("."));
		}
		
		// store file path and file name to send to FE
		fileMetaModelData.put("fileLocation", filePath);
		fileMetaModelData.put("fileName", file);
		fileMetaModelData.put("startCount", startRow);
		if (getEndRowCount) {
			fileMetaModelData.put("endCount", endRow);
		}
		fileMetaModelData.put("dataTypes", dataTypeMap);
		// fileMetaModelData.put("additionalDataTypes",
		// predictor.getAdditionalDataTypeMap());
		fileMetaModelData.put("additionalInfo", additionalInfo);
		// store auto modified header names
		fileMetaModelData.put("headerModifications", helper.getChangedHeaders());
		// need to close the helper
		helper.clear();

		return fileMetaModelData;
	}

	/**
	 * Get the end row count from file
	 * 
	 * @return
	 */
	private boolean getRowCount() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[2]);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}
}
