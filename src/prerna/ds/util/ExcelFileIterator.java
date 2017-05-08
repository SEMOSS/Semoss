package prerna.ds.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.QueryStruct;
import prerna.poi.main.helper.XLFileHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ExcelFileIterator extends AbstractFileIterator {

	private XLFileHelper helper;
	private int[] headerIndices;
	private String sheetToLoad;
	
	public ExcelFileIterator(String fileLocation, String sheetToLoad, QueryStruct qs, Map<String, String> dataTypeMap) {
		this.helper = new XLFileHelper();
		this.helper.parse(fileLocation);	
		this.sheetToLoad = sheetToLoad;
		this.dataTypeMap = dataTypeMap;
		
		setSelectors(qs.selectors);
		setFilters(qs.andfilters);
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.types = new String[this.headers.length];
			for(int j = 0; j < this.headers.length; j++) {
				this.types[j] = dataTypeMap.get(this.headers[j]);
			}
		}
		else {
			setUnknownTypes();
		}
		
		// need to grab the first row upon initialization 
		getNextRow();
	}
	
	private void setUnknownTypes() {
		this.types = this.helper.predictRowTypes(this.sheetToLoad, this.headerIndices);
		int numHeaders = this.headers.length;

		this.dataTypeMap = new Hashtable<String, String>();
		for(int i = 0; i < numHeaders; i++) {
			this.dataTypeMap.put(this.headers[i], types[i]);
		}
	}

	public void getNextRow() {
		String[] row = this.helper.getNextRow(this.sheetToLoad, this.headerIndices);
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
				row = this.helper.getNextRow(this.sheetToLoad, this.headerIndices);
			}
		}
		
		this.nextRow = newRow;
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
			
			this.headers = orderedSelectors;
			this.headerIndices = this.helper.getHeaderIndicies(this.sheetToLoad, orderedSelectors);
			this.helper.getNextRow(this.sheetToLoad, this.headerIndices); // after redoing the selectors, we need to skip the headers 
		} else {
			this.headers = allHeaders;
			this.headerIndices = new int[this.headers.length];
			for(int i = 0; i < this.headers.length; i++) {
				this.headerIndices[i] = i;
			}
		}
	}

	@Override
	public void resetHelper() {
		this.helper.reset();
	}

	@Override
	public void clearHelper() {
		this.helper.clear();
	}
}
