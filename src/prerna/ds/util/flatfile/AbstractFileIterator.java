package prerna.ds.util.flatfile;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.IFileIterator;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractFileIterator implements IFileIterator {

	private static final Logger classLogger = LogManager.getLogger(AbstractFileIterator.class);

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
	
	protected long limit = -1;
	protected long offset = -1;

	protected long curOffset = 0;
	protected long curLimit = 0;
	
	@Override
	public boolean hasNext() {
		if(offset > 0) {
			while(curOffset++ < offset) {
				getNextRow();
				if(nextRow == null) {
					// drops the file connection
					try {
						close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
					return false;
				}
			}
		}
		if(limit > 0) {
			if(curLimit > limit) {
				return false;
			}
		}
		if(nextRow == null) {
			// drops the file connection
			try {
				close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			return false;
		}
		return true;
	}
	
	@Override
	public IHeadersDataRow next() {
		curLimit++;
		
		Object[] row = nextRow;
		getNextRow();

		// couple of things to take care of here
		Object[] cleanRow = cleanRow(row, types, additionalTypes);
		IHeadersDataRow nextData = new HeadersDataRow(this.headers, cleanRow, row);
		return nextData;
	}
	
	protected Object[] cleanRow(Object[] row, SemossDataType[] types, String[] additionalTypes) {
		Object[] cleanRow = new Object[row.length];
		for(int i = 0; i < row.length; i++) {
			SemossDataType type = types[i];
			String val = row[i].toString().trim();
			// try to get correct type
			if(type == SemossDataType.INT) {
				// so we are consistent with the predict types
				cleanRow[i] = Utility.getInteger(val.replaceAll("[$,\\s]", ""));
			} else if(type == SemossDataType.DOUBLE) {
				// so we are consistent with the predict types
				cleanRow[i] = Utility.getDouble(val.replaceAll("[$,\\s]", ""));
			} else if(type == SemossDataType.BOOLEAN) {
				if(val.equals("null")) {
					cleanRow[i] = null;
				} else {
					cleanRow[i] = Boolean.parseBoolean(val);
				}
			} else if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				if(val.equals("null")) {
					cleanRow[i] = null;
				} else {
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
				}
			} else {
				cleanRow[i] = val; //Utility.cleanString(val, true, true, false);
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
		try {
			reset();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
		try {
			close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
	public void setEngine(IDatabaseEngine engine) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public IDatabaseEngine getEngine() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean flushable() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public String flush() {
		// TODO Auto-generated method stub
		return null;
	}
}
