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
	
	public FileIterator(String fileLoc, char delimiter, QueryStruct qs, Map<String, String> dataTypeMap) {
		this.helper = new CSVFileHelper();
		filters = new HashMap<String, Set<Object>>();
		helper.setDelimiter(delimiter);
		helper.parse(fileLoc);
		
		setSelectors(qs.getSelectors());
		setFilters(qs.andfilters);
		headers = helper.getHeaders();
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.dataTypeMap = dataTypeMap;
			
			this.types = new String[headers.length];
			for(int j = 0; j < headers.length; j++) {
				this.types[j] = dataTypeMap.get(headers[j]);
			}
			
			helper.parseColumns(headers);
		} else {
			this.dataTypeMap = new HashMap<String, String>();
			String[] allHeaders = helper.getAllCSVHeaders();
			this.types = helper.predictTypes();
			for(int i = 0; i < types.length; i++) {
				this.dataTypeMap.put(allHeaders[i], types[i]);
			}
			
			// need to redo types to be only those in the selectors
			this.types = new String[headers.length];
			for(int i = 0; i < headers.length; i++) {
				this.types[i] = this.dataTypeMap.get(headers[i]);
			}
			setSelectors(qs.getSelectors());
		}
		
		getNextRow(); // this will get the first row of the file
//		nextRow = helper.getNextRow();
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

		Object[] cleanRow = new Object[row.length];
		for(int i = 0; i < row.length; i++) {
			String type = types[i];
			if(type.contains("DOUBLE")) {
				try {
					//added to remove $ and , in data and then try parsing as Double
					row[i] = row[i].replaceAll("$,", "");
					cleanRow[i] = Double.parseDouble(row[i].trim());
				} catch(NumberFormatException ex) {
					//do nothing
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
