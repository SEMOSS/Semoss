package prerna.ds.util;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.NounMetadata;
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
	
	protected String[] headers;
	protected String[] types;
	protected String[] nextRow;
	
	protected GenRowFilters filters;
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
	
	protected void setFilters(GenRowFilters genRowFilters) {
		filters = genRowFilters;
		
	}
	
	protected boolean filterColToValues(NounMetadata leftComp, NounMetadata rightComp, String[] row, String comparator, int rowIndex) {
		List<Object> objects = new Vector<Object>();
		// the filtered values will either come in as a list or a single object,
		// convert everything to list
		if (rightComp.getValue() instanceof List) {
			objects.addAll((List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}

		boolean isValid = true;
		String leftValue = leftComp.getValue().toString();
		// get the data type of the filter (String, Number, or Date)
		SemossDataType valType = SemossDataType.convertStringToDataType(dataTypeMap.get(headers[rowIndex]));
		// Compare csv String row
		Vector<String> stringVals = new Vector<String>();

		// convert all filtered values to string to compare to csv values (which
		// are all strings)
		for (Object val : objects) {
			stringVals.add(val.toString());
		}
		if (comparator.equals("==") || comparator.equals("=")) {

			// equals case, see if the row value at the column index is
			// contained in the list of filtered values
			if (SemossDataType.convertStringToDataType(this.dataTypeMap.get(leftValue)) == SemossDataType.STRING) {
				if (stringVals.contains(Utility.cleanString(row[rowIndex], true))) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (stringVals.contains(row[rowIndex])) {
				// compare for non-string types
				isValid = isValid && true;
			} else {
				isValid = isValid && false;
			}
		} else if (comparator.equals("!=")) {
			// not equals case
			if (SemossDataType.convertStringToDataType(this.dataTypeMap.get(leftValue)) == SemossDataType.STRING) {

				if (!stringVals.contains(Utility.cleanString(row[rowIndex], true))) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (!stringVals.contains(row[rowIndex])) {
				isValid = isValid && true;
			} else {
				isValid = isValid && false;
			}
		} else if (comparator.equals(">")) {
			String filterVal = stringVals.get(0);
			if (valType.equals(SemossDataType.INT) || valType.equals(SemossDataType.DOUBLE)) {
				// parse strings (row value and filter value) into doubles for comparison
				if (Utility.getDouble(row[rowIndex]) > Utility.getDouble(filterVal)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (valType.equals(SemossDataType.STRING)) {
				// invalid case - can't do MB > 'Test', needs to be numeric
				isValid = false;
			} else if (valType.equals(SemossDataType.DATE)) {
				// get Java dates from string and compare
				Date filterDate = Utility.getDateAsDateObj(filterVal);
				Date rowDate = Utility.getDateAsDateObj(row[rowIndex]);
				if (rowDate.after(filterDate)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}

			}

		} else if (comparator.equals("<")) {
			String filterVal = stringVals.get(0);
			if (valType.equals(SemossDataType.INT) || valType.equals(SemossDataType.DOUBLE)) {
				// parse strings (row value and filter value) into doubles for comparison
				if (Utility.getDouble(row[rowIndex]) < Utility.getDouble(filterVal)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (valType.equals(SemossDataType.STRING)) {
				// invalid case - can't do MB > 'Test', needs to be numeric
				isValid = false;
			} else if (valType.equals(SemossDataType.DATE)) {
				// get Java dates from string and compare
				Date filterDate = Utility.getDateAsDateObj(filterVal);
				Date rowDate = Utility.getDateAsDateObj(row[rowIndex]);
				if (rowDate.before(filterDate)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}

			}
		} else if (comparator.equals(">=")) {
			String filterVal = stringVals.get(0);
			if (valType.equals(SemossDataType.INT) || valType.equals(SemossDataType.DOUBLE)) {
				if (Utility.getDouble(row[rowIndex]) >= Utility.getDouble(filterVal)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (valType.equals(SemossDataType.STRING)) {
				isValid = false;
			} else if (valType.equals(SemossDataType.DATE)) {
				Date filterDate = Utility.getDateAsDateObj(filterVal);
				Date rowDate = Utility.getDateAsDateObj(row[rowIndex]);
				if (rowDate.after(filterDate) || rowDate.equals(filterDate)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}

			}
		} else if (comparator.equals("<=")) {
			String filterVal = stringVals.get(0);
			if (valType.equals(SemossDataType.INT) || valType.equals(SemossDataType.DOUBLE)) {
				if (Utility.getDouble(row[rowIndex]) <= Utility.getDouble(filterVal)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}
			} else if (valType.equals(SemossDataType.STRING)) {
				isValid = false;
			} else if (valType.equals(SemossDataType.DATE)) {
				Date filterDate = Utility.getDateAsDateObj(filterVal);
				Date rowDate = Utility.getDateAsDateObj(row[rowIndex]);
				if (rowDate.before(filterDate) || rowDate.equals(filterDate)) {
					isValid = isValid && true;
				} else {
					isValid = isValid && false;
				}

			}
		}

		return isValid;
	}
}
