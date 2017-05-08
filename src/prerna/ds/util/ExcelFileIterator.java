package prerna.ds.util;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.QueryStruct;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.poi.main.helper.XLFileHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ExcelFileIterator implements IFileIterator {

	private XLFileHelper helper;
	private String[] headers;
	private String[] types;
	private String[] nextRow;
	
	private Map<String, Set<Object>> filters;
	private Map<String, String> dataTypeMap;
	
	private int numRecords = -1;
	private String sheetToLoad;
	
	public ExcelFileIterator(String fileLocation, String sheetToLoad, QueryStruct qs, Map<String, String> dataTypeMap) {
		this.helper = new XLFileHelper();
		this.helper.parse(fileLocation);
		this.sheetToLoad = sheetToLoad;
		this.dataTypeMap = dataTypeMap;
		
		setSelectors(qs.selectors);
		setFilters(qs.andfilters);
		
		this.headers = this.helper.getHeaders(this.sheetToLoad);
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.types = new String[this.headers.length];
			for(int j = 0; j < this.headers.length; j++) {
				this.types[j] = dataTypeMap.get(this.headers[j]);
			}
		}
//		else {
//			setUnknownTypes(fileIterator);
//			fileIterator.setSelectors(qs.getSelectors());
//		}
		
		// need to grab the first row upon initialization 
		getNextRow();
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextRow == null) {
			this.helper.clear();
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
	
	public void getNextRow() {
		String[] row = this.helper.getNextRow(this.sheetToLoad);
		if(this.filters == null || this.filters.isEmpty()) {
			this.nextRow = row;
			return;
		}
		
		String[] newRow = null;
		while(newRow == null && (row != null)) {
			for(int i = 0; i < row.length; i++) {
				Set<Object> nextSet = this.filters.get(headers[i]);
				if(nextSet != null ){
					if(this.dataTypeMap.get(headers[i]).toUpperCase().startsWith("VARCHAR")) {
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
				row = this.helper.getNextRow(this.sheetToLoad);
			}
		}
		
		this.nextRow = newRow;
	}

	private void setFilters(Map<String, Map<String, List>> andfilters) {
		for(String column : andfilters.keySet()) {
			Map<String, List> filterValues = andfilters.get(column);
			Set<Object> values = new HashSet<>();
			for(String comparator : filterValues.keySet()) {
				List vals = filterValues.get(comparator);
				values.addAll(vals);
			}
			
			this.filters.put(column, values);
		}
	}
	
	private void setSelectors(Map<String, List<String>> selectorSet) {
		if(selectorSet.isEmpty()) {
			return; // if no selectors, return everything
		}
		String[] selectors = selectorSet.keySet().toArray(new String[]{});
		String[] allHeaders = this.helper.getHeaders(this.sheetToLoad);
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
			
//			this.helper.parseColumns(orderedSelectors);
			this.helper.getNextRow(this.sheetToLoad); // after redoing the selectors, we need to skip the headers 
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
		this.helper.reset();
		getNextRow();
		return overLimit;
	}
	
	public int getNumRecords() {
		return this.numRecords;
	}

	@Override
	public String[] getTypes() {
		return this.types;
	}

	@Override
	public String[] getHeaders() {
		return this.headers;
	}
	
}
