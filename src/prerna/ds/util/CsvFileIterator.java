package prerna.ds.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.ds.QueryStruct;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class CsvFileIterator extends AbstractFileIterator{

	private CSVFileHelper helper;
	
	private CsvFileIterator() {
		
	}
	
	/**
	 * Shifting this to a new framework
	 * Java cannot distinguish between the data inside a Map
	 * But we want to differentiate between a map with all strings or with the formal IMetaData types
	 * TODO: Should go to only using IMetaData types
	 * @param type
	 * @param fileLoc
	 * @param delimiter
	 * @param qs
	 * @param dataTypeMap
	 * @param newHeaders 
	 * @return
	 */
	public static CsvFileIterator createInstance(FILE_DATA_TYPE type, String fileLoc, char delimiter, QueryStruct qs, Map<String, ?> dataTypeMap, Map<String, String> newHeaders) {
		if(type == FILE_DATA_TYPE.STRING) {
			return createStringFileIterator(fileLoc, delimiter, qs, (Map<String, String>) dataTypeMap, newHeaders);
		} else if(type == FILE_DATA_TYPE.META_DATA_ENUM) {
			return createEnumFileIterator(fileLoc, delimiter, qs, (Map<String, IMetaData.DATA_TYPES>) dataTypeMap, newHeaders);
		} else {
			throw new IllegalArgumentException("Unknown FileIterator type to generate");
		}
	}
	
	private static CsvFileIterator createStringFileIterator(String fileLoc, char delimiter, QueryStruct qs, Map<String, String> dataTypeMap, Map<String, String> newHeaders) {
		CsvFileIterator fileIterator = createDefualtFileIteratorParameters(fileLoc, qs, delimiter, newHeaders);
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			fileIterator.dataTypeMap = dataTypeMap;
			
			fileIterator.types = new String[fileIterator.headers.length];
			for(int j = 0; j < fileIterator.headers.length; j++) {
				fileIterator.types[j] = dataTypeMap.get(fileIterator.headers[j]);
			}
			
			fileIterator.helper.parseColumns(fileIterator.headers);
		} else {
			setUnknownTypes(fileIterator);
			fileIterator.setSelectors(qs.getSelectors());
		}
		
		fileIterator.getNextRow(); // this will get the first row of the file
//		nextRow = helper.getNextRow();
		
		return fileIterator;
	}
	
	private static CsvFileIterator createEnumFileIterator(String fileLoc, char delimiter, QueryStruct qs, Map<String, IMetaData.DATA_TYPES> dataTypeMap, Map<String, String> newHeaders) {
		CsvFileIterator fileIterator = createDefualtFileIteratorParameters(fileLoc, qs, delimiter, newHeaders);

		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			fileIterator.types = new String[fileIterator.headers.length];
			for(int j = 0; j < fileIterator.headers.length; j++) {
				String header =  fileIterator.headers[j];
				String dataType = Utility.convertDataTypeToString(dataTypeMap.get(header));
				fileIterator.dataTypeMap.put(header, dataType);
				fileIterator.types[j] = dataType;
			}
			
			fileIterator.helper.parseColumns(fileIterator.headers);
		} else {
			setUnknownTypes(fileIterator);
			fileIterator.setSelectors(qs.getSelectors());
		}
		
		fileIterator.getNextRow(); // this will get the first row of the file
//		nextRow = helper.getNextRow();
		
		return fileIterator;
	}
	
	/**
	 * Creates the defualt file iterator
	 * @param fileLoc
	 * @param qs
	 * @param delimiter
	 * @param newHeaders 
	 * @return
	 */
	private static CsvFileIterator createDefualtFileIteratorParameters(String fileLoc, QueryStruct qs, char delimiter, Map<String, String> newHeaders) {
		CsvFileIterator fileIterator = new CsvFileIterator();

		fileIterator.helper = new CSVFileHelper();
		fileIterator.filters = new HashMap<String, Set<Object>>();
		fileIterator.helper.setDelimiter(delimiter);
		fileIterator.helper.parse(fileLoc);
		
		// set the user defined headers
		if(newHeaders != null && !newHeaders.isEmpty()) {
			fileIterator.newHeaders = newHeaders;
			fileIterator.helper.modifyCleanedHeaders(newHeaders);
		}
		
		fileIterator.setSelectors(qs.getSelectors());
		fileIterator.setFilters(qs.andfilters);
		fileIterator.headers = fileIterator.helper.getHeaders();
		fileIterator.dataTypeMap = new HashMap<String, String>();

		return fileIterator;
	}
	
	/**
	 * Determine the data types by parsing through the file
	 * @param fileIterator
	 */
	private static void setUnknownTypes(CsvFileIterator fileIterator) {
		fileIterator.dataTypeMap = new HashMap<String, String>();
		String[] allHeaders = fileIterator.helper.getAllCSVHeaders();
		fileIterator.types = fileIterator.helper.predictTypes();
		for(int i = 0; i < fileIterator.types.length; i++) {
			fileIterator.dataTypeMap.put(allHeaders[i], fileIterator.types[i]);
		}
		
		// need to redo types to be only those in the selectors
		fileIterator.types = new String[fileIterator.headers.length];
		for(int i = 0; i < fileIterator.headers.length; i++) {
			fileIterator.types[i] = fileIterator.dataTypeMap.get(fileIterator.headers[i]);
		}
	}
	
	public void getNextRow() {
		String[] row = helper.getNextRow();
		if(filters == null || filters.isEmpty()) {
			this.nextRow = row;
			return;
		}
		
		String[] newRow = null;
		while(newRow == null && (row != null)) {
			for(int i = 0; i < row.length; i++) {
				Set<Object> nextSet = filters.get(headers[i]);
				if(nextSet != null ){
					if(Utility.convertStringToDataType(this.dataTypeMap.get(headers[i])) == DATA_TYPES.STRING) {
						if(nextSet.contains(Utility.cleanString(row[i], true))) {
							newRow = row;
						} else {
							newRow = null;
							break;
						}
					} else if(nextSet.contains(row[i])) {
						newRow = row;
					}
					else {
						newRow = null;
						break;
					}
				}
			}
			if(newRow == null) {
				row = helper.getNextRow();
			}
		}
		
		this.nextRow = newRow;
	}
	
	private void setSelectors(Map<String, List<String>> selectorSet) {
		if(selectorSet.isEmpty()) {
			return; // if no selectors, return everything
		}
		String[] selectors = selectorSet.keySet().toArray(new String[]{});
		String[] allHeaders = this.helper.getHeaders();
		if(allHeaders.length != selectors.length) {
			// order the selectors
			// all headers will be ordered
			String[] orderedSelectors = new String[selectors.length];
			int counter = 0;
			for(String header : allHeaders) {
				if(ArrayUtilityMethods.arrayContainsValue(selectors, header)) {
					orderedSelectors[counter] = header;
					counter++;
				}
			}
			
			this.helper.parseColumns(orderedSelectors);
			this.helper.getNextRow(); // after redoing the selectors, we need to skip the headers 
		}
	}
	
	public char getDelimiter() {
		return this.helper.getDelimiter();
	}
	
	@Override
	public void resetHelper() {
		this.helper.reset(false);
	}

	@Override
	public void clearHelper() {
		this.helper.clear();
	}
	
}
