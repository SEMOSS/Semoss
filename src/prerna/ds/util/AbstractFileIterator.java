package prerna.ds.util;

import java.io.File;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.Utility;

public abstract class AbstractFileIterator implements IFileIterator{

	/*
	 * Trying to hold a common interface between loading via csv file
	 * or loading via excel file
	 * 
	 * Tried to shift the methods that do not use CSVFileHelper or XLFileHelper
	 * into this class to help reduce redundancies within the code
	 * 
	 */
	
	protected String fileLocation;
	
	protected String[] headers;
	protected String[] types;
	protected String[] nextRow;
	
	protected Map<String, Set<Object>> filters;
	protected Map<String, String> dataTypeMap;
	protected Map<String, String> newHeaders;
	protected int numRecords = -1;
	
	public abstract void getNextRow();
	
	/**
	 * Reset the connection to the file
	 * As if no iterating through
	 * the file has ever occurred
	 */
	public abstract void resetHelper();
	
	/**
	 * Drop the connection to the file
	 */
	public abstract void clearHelper();
	
	@Override
	public boolean hasNext() {
		if(nextRow == null) {
			// drops the file connection
			clearHelper();
			return false;
		}
		return true;
	}
	
	@Override
	public IHeadersDataRow next() {
		String[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		Object[] cleanRow = cleanRow(row, row);
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, cleanRow);
		return nextData;
	}
	
	protected Object[] cleanRow(String[] row, String[] types) {
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
					val = val.replaceAll("[^0-9\\.E]", "");
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
		
		return cleanRow;
	}

	@Override
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
		resetHelper();
		getNextRow();
		return overLimit;
	}
	
	@Override
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
	
	public void deleteFile() {
		clearHelper();
		File file = new File(this.fileLocation);
		file.delete();
	}
	
	public String getFileLocation() {
		return this.fileLocation;
	}
	
	protected void setFilters(Map<String, Map<String, List>> andfilters) {
		filters = new Hashtable<String, Set<Object>>();
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
}
