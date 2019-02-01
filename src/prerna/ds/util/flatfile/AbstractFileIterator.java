package prerna.ds.util.flatfile;

import java.io.File;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.IFileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.util.Utility;

public abstract class AbstractFileIterator implements IFileIterator {

	/*
	 * Trying to hold a common interface between loading via csv file
	 * or loading via excel file
	 * 
	 * Tried to shift the methods that do not use CSVFileHelper or XLFileHelper
	 * into this class to help reduce redundancies within the code
	 * 
	 */
	
	protected String fileLocation;
	
	// variables used by the iterator
	protected String[] headers;
	protected SemossDataType[] types;
	protected String[] additionalTypes;
	protected Object[] nextRow;
	
	// variables set by qs
	protected Map<String, String> newHeaders;
	protected Map<String, String> dataTypeMap;
	protected Map<String, String> additionalTypesMap;

	protected long numRecords = -1;
	
	public abstract void getNextRow();
	
	@Override
	public boolean hasNext() {
		if(nextRow == null) {
			// drops the file connection
			cleanUp();
			return false;
		}
		return true;
	}
	
	@Override
	public IHeadersDataRow next() {
		Object[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		Object[] cleanRow = cleanRow(row, types, additionalTypes);
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, cleanRow);
		return nextData;
	}
	
	protected Object[] cleanRow(Object[] row, SemossDataType[] types, String[] additionalTypes) {
		Object[] cleanRow = new Object[row.length];
		for(int i = 0; i < row.length; i++) {
			SemossDataType type = types[i];
			String val = row[i].toString().trim();
			// try to get correct type
			if(type == SemossDataType.INT) {
				try {
					//added to remove $ and , in data and then try parsing as Double
					int mult = 1;
					if(val.startsWith("(") || val.startsWith("-")) // this is a negativenumber
						mult = -1;
					val = val.replaceAll("[^0-9\\.E]", "");
					cleanRow[i] = mult * Integer.parseInt(val.trim());
				} catch(NumberFormatException ex) {
					//do nothing
					cleanRow[i] = null;
				}
			} else if(type == SemossDataType.DOUBLE) {
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
			} else if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				String additionalTypeData = additionalTypes[i];
				
				// if we have additional data format for the date
				// send the date object
				Object date = null;
				if(additionalTypeData != null) {
					date = new SemossDate(val, additionalTypeData);
				} else {
					date = val;
				}
				cleanRow[i] = date;
			} else if(type == SemossDataType.DATE) {
				String additionalTypeData = additionalTypes[i];
				
				// if we have additional data format for the date
				// send the date object
				SemossDate date = null;
				if(additionalTypeData != null) {
					date = new SemossDate(val, additionalTypeData);
				} else {
					date = SemossDate.genDateObj(val);
				}
				cleanRow[i] = date;
			} else if(type == SemossDataType.TIMESTAMP) {
				String additionalTypeData = additionalTypes[i];
				
				// if we have additional data format for the date
				// send the date object
				SemossDate date = null;
				if(additionalTypeData != null) {
					date = new SemossDate(val, additionalTypeData);
				} else {
					date = SemossDate.genTimeStampDateObj(val);
				}
				cleanRow[i] = date;
			} else {
				cleanRow[i] = Utility.cleanString(val, true, true, false);
			}
		}
		
		return cleanRow;
	}

	@Override
	public boolean getNumRecordsOverSize(long limit) {
		boolean overLimit = false;
		long counter = 0;
		while(this.hasNext()) {
			this.getNextRow();
			counter += this.headers.length;
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
		reset();
		getNextRow();
		return overLimit;
	}
	
	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}

	@Override
	public String[] getHeaders() {
		return this.headers;
	}
	
	@Override
	public long getNumRows() {
		return this.numRecords / getHeaders().length;
	}
	
	@Override
	public long getNumRecords() {
		return this.numRecords;
	}
	
	public void deleteFile() {
		cleanUp();
		File file = new File(this.fileLocation);
		file.delete();
	}
	
	public String getFileLocation() {
		return this.fileLocation;
	}
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void setEngine(IEngine engine) {
		// TODO Auto-generated method stub
		
	}
	
}
