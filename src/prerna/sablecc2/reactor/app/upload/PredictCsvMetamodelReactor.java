package prerna.sablecc2.reactor.app.upload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;

public class PredictCsvMetamodelReactor extends AbstractReactor {
	
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public PredictCsvMetamodelReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, UploadInputUtility.DELIMITER, UploadInputUtility.ROW_COUNT };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get csv file path
		String filePath = UploadInputUtility.getFilePath(this.store);
		// get delimiter
		String delimiter = UploadInputUtility.getDelimiter(this.store);
		char delim = delimiter.charAt(0);

		// set csv file helper
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(filePath);

		return new NounMetadata(autoGenerateMetaModel(helper), PixelDataType.MAP);
	}

	/**
	 * predict the meta model
	 */
	private Map<String, Object> autoGenerateMetaModel(CSVFileHelper helper) {
		// return map with file metamodel
		Map<String, Object> fileMetaModelData = new HashMap<String, Object>();
		String[] columnHeaders = helper.getHeaders();
		Map<String, SemossDataType> dataTypeMap = new LinkedHashMap<String, SemossDataType>();
		Map<String, String> additionalDataTypeMap = new LinkedHashMap<String, String>();

		// predict datatypes and additional types
		Object[][] dataTypes = helper.predictTypes();
		int size = columnHeaders.length;
		for (int colIdx = 0; colIdx < size; colIdx++) {
			Object[] prediction = dataTypes[colIdx];
			dataTypeMap.put(columnHeaders[colIdx], (SemossDataType) prediction[0]);
			if (prediction[1] != null) {
				additionalDataTypeMap.put(columnHeaders[colIdx], (String) prediction[1]);
			}
		}

		// get data from csv to predict types
		List<String[]> data = new ArrayList<>(500);
		String[] cells = null;
		int count = 1;
		// predict meta model from limit row count
		int limit = 500;
		// get end row count
		boolean getEndRowCount = UploadInputUtility.getRowCount(this.store);
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

		fileMetaModelData.put("startCount", 2);
		if (getEndRowCount) {
			fileMetaModelData.put("endCount", endRow);
		}
		fileMetaModelData.put("dataTypes", dataTypeMap);
		fileMetaModelData.put("additionalDataTypes", additionalDataTypeMap);
		// store auto modified header names
		fileMetaModelData.put("headerModifications", helper.getChangedHeaders());

		Map<String, Set<String>> matches = new HashMap<>(columnHeaders.length);
		Map<String, Boolean> columnPropMap = new HashMap<>(columnHeaders.length);
		for (String header : columnHeaders) {
			columnPropMap.put(header, false);
		}

		for (int i = 0; i < columnHeaders.length; i++) {
			SemossDataType datatype = dataTypeMap.get(columnHeaders[i]);
			// run comparisons for strings
			if (datatype == SemossDataType.STRING) {
				runAllComparisons(columnHeaders, i, matches, columnPropMap, dataTypeMap, data);
			} else {
				// run comparisons for non string types
				runAllComparisons(columnHeaders, i, matches, columnPropMap, dataTypeMap, data);
			}
		}

		// Format metamodel data
		Map<String, Object> propFileData = new HashMap<>();
		List<Map<String, Object>> relationMapList = new ArrayList<>();
		Map<String, List<String>> nodePropMap = new HashMap<>();
		for (String subject : matches.keySet()) {
			Set<String> set = matches.get(subject);
			for (String object : set) {
				SemossDataType datatype = dataTypeMap.get(object);
				String[] subjectArr = { subject };
				String[] objectArr = { object };
				if (datatype == SemossDataType.STRING) {
					Map<String, Object> relMap = new HashMap<>();
					String relName = subject + "_" + object;
					relMap.put(Constants.FROM_TABLE, subject);
					relMap.put(Constants.TO_TABLE, object);
					relMap.put(Constants.REL_NAME, relName);
					relationMapList.add(relMap);
				} else {
					List<String> properties = new ArrayList<>();
					if (nodePropMap.containsKey(subject)) {
						properties = nodePropMap.get(subject);
					}
					properties.add(object);
					nodePropMap.put(subject, properties);
				}
			}
		}
		propFileData.put(Constants.RELATION, relationMapList);
		propFileData.put(Constants.NODE_PROP, nodePropMap);
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
			file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(), filePath.lastIndexOf("."));
		}

		// store file path and file name to send to FE
		fileMetaModelData.put("fileLocation", filePath);
		fileMetaModelData.put("fileName", file);
		helper.clear();
		return fileMetaModelData;
	}

	/**
	 * 
	 * @param columnHeaders
	 *            - the column headers in the csv
	 * @param firstColIndex
	 *            - the column which we are comparing to other columns
	 * @param matches
	 * @param columnPropMap
	 * @param dataTypeMap
	 * @param data
	 */
	private void runAllComparisons(String[] columnHeaders, int firstColIndex, Map<String, Set<String>> matches, Map<String, Boolean> columnPropMap, Map<String, SemossDataType> dataTypeMap, List<String[]> data) {
		for(int i = 0; i < columnHeaders.length; i++) {
			//don't compare a column to itself
			if(i == firstColIndex) continue;

			String firstColumn = columnHeaders[firstColIndex];
			String secondColumn = columnHeaders[i];

			//need to make sure second column does not have first column as a a property already
			if(!matches.containsKey(secondColumn) || !matches.get(secondColumn).contains(firstColumn)) {
				if(!columnPropMap.get(secondColumn) && compareCols(firstColIndex, i, data)) {
					//we have a match
					boolean useInverse = false;
					int firstColIndexInCSV = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, firstColumn);
					int secondColIndexInCSV = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, secondColumn);
					if(firstColIndexInCSV > secondColIndexInCSV) {
						//try to see if inverse order is better
						//but first, check to make sure the second column in not a double or date
						SemossDataType dataType = dataTypeMap.get(secondColumn);
						if(dataType == SemossDataType.STRING) {
							if(!columnPropMap.get(firstColumn) && compareCols(i, firstColIndex, data)) {
								//use reverse order
								useInverse = true;
								if(matches.containsKey(secondColumn)) {
									matches.get(secondColumn).add(firstColumn);
								} else {
									Set<String> set = new HashSet<String>(1);
									set.add(firstColumn);
									matches.put(secondColumn, set);
								}
								columnPropMap.put(firstColumn, true);
							}
						}
					}

					if(!useInverse) {
						if(matches.containsKey(firstColumn)) {
							matches.get(firstColumn).add(secondColumn);
						} else {
							Set<String> set = new HashSet<String>(1);
							set.add(secondColumn);
							matches.put(firstColumn, set);
						}
						columnPropMap.put(secondColumn, true);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	private boolean compareCols(int firstIndex, int secondIndex, List<String[]> data) {
		Map<Object, Object> values = new HashMap<>();
		for(Object[] row : data) {
			Object firstValue = row[firstIndex];
			Object secondValue = row[secondIndex];
			if(values.containsKey(firstValue)) {
				if(!values.get(firstValue).equals(secondValue)) {
					return false;
				}
			} else {
				values.put(firstValue, secondValue);
			}
		}
		return true;
	}
}


