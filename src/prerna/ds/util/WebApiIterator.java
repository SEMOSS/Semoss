package prerna.ds.util;

import java.io.IOException;
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
import prerna.poi.main.helper.AmazonApiHelper;
import prerna.poi.main.helper.ImportApiHelper;
import prerna.poi.main.helper.WebAPIHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class WebApiIterator implements Iterator<IHeadersDataRow>{

	private WebAPIHelper helper;
	private String[] headers;
	private String[] types;
	private String[] nextRow;
	
	private Map<String, Set<Object>> filters;
	private Map<String, String> dataTypeMap;
	
	//delete this constructor after new PKQL processing is success
	/*public ImportApiIterator(String api, QueryStruct qs, Map<String, String> dataTypeMap) throws IOException {
		this.helper = new ImportApiHelper();
		filters = new HashMap<String, Set<Object>>();
		helper.setApi(api);
		helper.parse();
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.dataTypeMap = dataTypeMap;
			headers = dataTypeMap.keySet().toArray(new String[]{});
			types = new String[headers.length];
			for(int j = 0; j < headers.length; j++) {
				types[j] = dataTypeMap.get(headers[j]);
			}
			
			helper.getNextRow(); // next row is a header
		} else {
			this.dataTypeMap = new HashMap<String, String>();
			headers = helper.getHeaders();//change to headers
			types = helper.predictTypes();
			for(int i = 0; i < types.length; i++) {
				this.dataTypeMap.put(headers[i], types[i]);
			}
		}
		
		setSelectors(qs.getSelectors());
		setFilters(qs.andfilters);
		getNextRow(); // this will get the first row of the file--but do we really need it here?

	}*/
	
	//For ImportIO pkql
	public WebApiIterator(String api) throws IOException {
		this.helper = new ImportApiHelper();
		filters = new HashMap<String, Set<Object>>();
		helper.setApiParam(api);
		helper.parse();
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.dataTypeMap = dataTypeMap;
			headers = dataTypeMap.keySet().toArray(new String[]{});
			types = new String[headers.length];
			for(int j = 0; j < headers.length; j++) {
				types[j] = dataTypeMap.get(headers[j]);
			}
			
			helper.getNextRow(); // next row is a header
		} else {
			this.dataTypeMap = new HashMap<String, String>();
			headers = helper.getHeaders();
			types = helper.predictTypes();
			for(int i = 0; i < types.length; i++) {
				this.dataTypeMap.put(headers[i], types[i]);
			}
		}
		
		QueryStruct qs = new QueryStruct();
		for(String header : headers) {
			qs.addSelector(header, null);
		}
		
		setSelectors(qs.getSelectors());
		setFilters(qs.andfilters);
		getNextRow(); // this will get the first row of the file--but do we really need it here?

	}
	
	/*
	 * For Amazon pkql
	 * @param OperationType - itemSearch [keywords - search keywords], itemLookup [keywords - ASIN]
	 */
	public WebApiIterator(String operationType, String keywords) throws IOException {
		this.helper = new AmazonApiHelper();
		filters = new HashMap<String, Set<Object>>();
		((AmazonApiHelper) helper).setOperationType(operationType);
		helper.setApiParam(keywords);//pass the keyword for ITemSearch -- can set operation type
		helper.parse();		
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			this.dataTypeMap = dataTypeMap;
			headers = dataTypeMap.keySet().toArray(new String[]{});
			types = new String[headers.length];
			for(int j = 0; j < headers.length; j++) {
				types[j] = dataTypeMap.get(headers[j]);
			}
			
			helper.getNextRow(); // next row is a header
		} else {
			this.dataTypeMap = new HashMap<String, String>();
			headers = helper.getHeaders();
			types = helper.predictTypes();
			for(int i = 0; i < types.length; i++) {
				this.dataTypeMap.put(headers[i], types[i]);
			}
		}
		
		QueryStruct qs = new QueryStruct();
		for(String header : headers) {
			qs.addSelector(header, null);
		}
		
		setSelectors(qs.getSelectors());
		setFilters(qs.andfilters);
		getNextRow(); // this will get the first row of the file--but do we really need it here?

	}
	
	@Override
	public boolean hasNext() {
		if(nextRow == null) {
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
					cleanRow[i] = Double.parseDouble(row[i].trim());
				} catch(NumberFormatException ex) {
					//do nothing
				}
			} else if((type.contains("DATE")) || (row[i] == null)) {
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
		}
	}
	

}
