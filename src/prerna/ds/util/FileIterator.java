package prerna.ds.util;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class FileIterator implements Iterator<IHeadersDataRow>{

	private CSVFileHelper helper;
	private String[] headers;
	private String[] types;
	private String[] nextRow;
	
	private Map<String, Set<Object>> filters;
	private Map<String, String> dataTypeMap;
	
	private int numRecords = -1;
	
	public enum FILE_DATA_TYPE {STRING, META_DATA_ENUM}
	
	private FileIterator() {
		
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
	 * @return
	 */
	public static FileIterator createInstance(FILE_DATA_TYPE type, String fileLoc, char delimiter, QueryStruct qs, Map<String, ?> dataTypeMap) {
		if(type == FILE_DATA_TYPE.STRING) {
			return createStringFileIterator(fileLoc, delimiter, qs, (Map<String, String>) dataTypeMap);
		} else if(type == FILE_DATA_TYPE.META_DATA_ENUM) {
			return createEnumFileIterator(fileLoc, delimiter, qs, (Map<String, IMetaData.DATA_TYPES>) dataTypeMap);
		} else {
			throw new IllegalArgumentException("Unknown FileIterator type to generate");
		}
	}
	
	private static FileIterator createStringFileIterator(String fileLoc, char delimiter, QueryStruct qs, Map<String, String> dataTypeMap) {
		FileIterator fileIterator = createDefualtFileIteratorParameters(fileLoc, qs, delimiter);
		
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
	
	private static FileIterator createEnumFileIterator(String fileLoc, char delimiter, QueryStruct qs, Map<String, IMetaData.DATA_TYPES> dataTypeMap) {
		FileIterator fileIterator = createDefualtFileIteratorParameters(fileLoc, qs, delimiter);

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
	 * @return
	 */
	private static FileIterator createDefualtFileIteratorParameters(String fileLoc, QueryStruct qs, char delimiter) {
		FileIterator fileIterator = new FileIterator();

		fileIterator.helper = new CSVFileHelper();
		fileIterator.filters = new HashMap<String, Set<Object>>();
		fileIterator.helper.setDelimiter(delimiter);
		fileIterator.helper.parse(fileLoc);
		
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
	private static void setUnknownTypes(FileIterator fileIterator) {
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
	
	@Override
	public boolean hasNext() {
		if(nextRow == null) {
			helper.clear();
			return false;
		}
		return true;
	}

	@Override
	public IHeadersDataRow next() {
		String[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		
		Object[] cleanRow = new Object[row.length];
		for(int i = 0; i < row.length; i++) {
			String type = types[i];
			if(type.contains("DOUBLE")) {
				String val = row[i].trim();
				try {
					//added to remove $ and , in data and then try parsing as Double
					int mult = 1;
					if(val.startsWith("(") || val.startsWith("-")) // this is a negativenumber
						mult = -1;
					val = val.replaceAll("[^0-9\\.]", "");
					cleanRow[i] = mult * Double.parseDouble(val.trim());
				} catch(NumberFormatException ex) {
					//do nothing
					cleanRow[i] = null;
				}
			} else if(type.contains("DATE")) {
				cleanRow[i] = row[i]; //TODO do i need to do anything for dates???
			} else {
				cleanRow[i] = Utility.cleanString(row[i], true, true, false);
			} 
		}
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, cleanRow);
		return nextData;
	}
	
	public String[] getHeaders() {
		return this.headers;
	}
	
	public String[] getTypes() {
		return this.types;
	}
	
	public void getNextRow() {
		String[] row = helper.getNextRow();
		if(filters == null || filters.isEmpty()) {
			this.nextRow = row;
			return;
		}
		
		//TODO: look into this filter logic
		String[] newRow = null;
		while(newRow == null && (row != null)) {
			for(int i = 0; i < row.length; i++) {
				Set<Object> nextSet = filters.get(headers[i]);
				if(nextSet != null ){
					if(dataTypeMap.get(headers[i]).toUpperCase().startsWith("VARCHAR")) {
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
	
	private void setFilters(Hashtable<String, Hashtable<String, Vector>> andfilters) {
		for(String column : andfilters.keySet()) {
			Hashtable<String, Vector> filterValues = andfilters.get(column);
			Set<Object> values = new HashSet<>();
			for(String comparator : filterValues.keySet()) {
				List vals = filterValues.get(comparator);
				values.addAll(vals);
			}
			
			this.filters.put(column, values);
		}
	}
	
	private void setSelectors(Hashtable<String, Vector<String>> selectorSet) {
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
	
	public void deleteFile() {
		this.helper.clear();
		File file = new File(this.helper.getFileLocation());
		file.delete();
	}
	
	public String getFileLocation() {
		return this.helper.getFileLocation();
	}
	
	public char getDelimiter() {
		return this.helper.getDelimiter();
	}
	
	public boolean numberRowsOverLimit(int limit) {
		boolean overLimit = false;
		int counter = 0;
		while(this.hasNext()) {
			this.getNextRow();
			counter++;
			if(counter > limit) {
				overLimit = true;
				break;
			}
		}
		
		// we only keep the number of records
		// if we actually iterated through everything
		// and found it not larger than the limit set
		if(!overLimit) {
			numRecords = counter;
		}
		
		// reset so we get the values again
		this.helper.reset(false);
		getNextRow();
		return overLimit;
	}
	
	public int getNumRecords() {
		return this.numRecords;
	}
	
}
