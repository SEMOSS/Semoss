package prerna.poi.main.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.poi.main.HeadersException;
import prerna.test.TestUtilityMethods;
import prerna.util.ArrayUtilityMethods;

@Deprecated
public class XLFileHelper {
	
	int colStarter = 0;
	
	private static final int NUM_ROWS_TO_PREDICT_TYPES = 1000;

	private	Workbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;

	// contains the string to the excel sheet object
	private Map <String, Sheet> sheetNames = new Hashtable<String, Sheet>();
	// gets the sheet name -> headers for sheet
	private Map <String, String[]> original_headers = new Hashtable<String, String[]>();
	// gets the sheet name -> headers for sheet
	private Map <String, String[]> clean_headers = new Hashtable<String, String[]>();
	// used for iterating through the sheet
	private Map <String, Integer> sheetCounter = new Hashtable<String, Integer>();
	
	// used to assimilate the properties...
	// TODO: need to come back and understand what that means...
	// has to do with prediction of what things are related but need to look more into this
	private Map <String, Vector<String>> allProps = new Hashtable <String, Vector<String>> ();

	
	/**
	 * Parse the new file passed
	 * @param fileLocation		The String location of the fileLocation
	 */
	public void parse(String fileLocation) {
		this.fileLocation = fileLocation;
		createParser();
	}
	
	
	/**
	 * opens the workbook and gets all the sheets
	 */
	private void createParser()
	{
		try {
			sourceFile = new FileInputStream(fileLocation);
			try {
				workbook = WorkbookFactory.create(sourceFile);
			} catch (EncryptedDocumentException e) {
				e.printStackTrace();
			}
			// get all the sheets
			int totalSheets = workbook.getNumberOfSheets();

			// store all the sheets
			for(int sheetIndex = 0;sheetIndex < totalSheets; sheetIndex++) {
				Sheet sheet = workbook.getSheetAt(sheetIndex);
				String nameOfSheet = sheet.getSheetName();
				sheetNames.put(nameOfSheet, sheet);
				
				String[] sheetHeaders = getSheetHeaders(sheet);
				if(sheetHeaders == null) {
					continue;
				}
				original_headers.put(nameOfSheet, sheetHeaders);
				
				// grab the headerChecker
				HeadersException headerChecker = HeadersException.getInstance();
				List<String> newUniqueCleanHeaders = new Vector<String>();
				
				int numCols = sheetHeaders.length;
				for(int colIdx = 0; colIdx < numCols; colIdx++) {
					String origHeader = sheetHeaders[colIdx];
					if(origHeader.trim().isEmpty()) {
						origHeader = "BLANK_HEADER";
					}
					String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueCleanHeaders);
					
					// now update the unique headers, as this will be used to match duplications
					newUniqueCleanHeaders.add(newHeader);
				}
				
				// now store the clean headers
				clean_headers.put(nameOfSheet, newUniqueCleanHeaders.toArray(new String[]{}));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String[] getHeaders(String sheetName) {
		return clean_headers.get(sheetName);
	}
	
	// this is in case the user has a dumb excel
	// where the top row is completely empty/null
	// find the first non-null row
	public String[] getSheetHeaders(Sheet sheet) {
		int counter = 0;
		Row headerRow = null;
		while(headerRow == null && counter < sheet.getLastRowNum()) {
			headerRow = sheet.getRow(counter);
			counter++;
		}
		
		// at this point, the sheet is empty and can't do anything
		if(headerRow == null) {
			sheetNames.remove(sheet.getSheetName());
			return null;
		}
		// get the headers
		int colLength = headerRow.getLastCellNum();
		Object[] sheetHeaders = getCells(headerRow, colLength);
		// set the new start for the getNextRow for this sheet
		sheetCounter.put(sheet.getSheetName(), counter);
		
		return Arrays.copyOf(sheetHeaders, sheetHeaders.length, String[].class);
	}
	
	/////////////////// START ALL NEXT ROWS ///////////////////
	
	public Object[] getNextRow(String sheetName) {
		Sheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		Object[] thisRow = null;
		while(thisRow == null && counter <= thisSheet.getLastRowNum()) {
			thisRow = getCells(thisSheet.getRow(counter), getHeaders(sheetName).length);
			counter++;		
		}
		
		// set counter back
		sheetCounter.put(sheetName, counter);
		
		// assimilate the properties
		// TODO: this logic isn't valid since we get the headers on instantiation of the instance
//		if(counter == 0) {
//			for(int colIndex = colStarter; colIndex < thisRow.length; colIndex++) {
//				putProp(thisRow[colIndex], sheetName);
//			}
//		}

		return thisRow;
	}
	
	private Object[] getCells(Row row, int totalCol) {
		int colLength = totalCol;
		Object[] cols = new Object[colLength];
		for(int colIndex = colStarter; colIndex < colLength; colIndex++) {
			Cell thisCell = row.getCell(colIndex);
			cols[colIndex] = getCell(thisCell);
		}	

		return cols;
	}
	
	/////////////////// END ALL NEXT ROWS ///////////////////

	/////////////////// START SPECIFIC HEADERS NEXT ROWS ///////////////////

	public Object[] getNextRow(String sheetName, String[] headersToGet) {
		String[] allHeaders = clean_headers.get(sheetName);
		if(allHeaders.length == headersToGet.length) {
			return getNextRow(sheetName);
		}
		
		Sheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		Object[] thisRow = null;
		while(thisRow == null && counter <= thisSheet.getLastRowNum()) {
			thisRow = getCells(thisSheet.getRow(counter), allHeaders, headersToGet);
			counter++;		
		}
		
		// set counter back
		sheetCounter.put(sheetName, counter);
		
		// assimilate the properties
		// TODO: this logic isn't valid since we get the headers on instantiation of the instance
//		if(counter == 0) {
//			for(int colIndex = colStarter; colIndex < thisRow.length; colIndex++) {
//				putProp(thisRow[colIndex], sheetName);
//			}
//		}
		
		return thisRow;
	}
	
	private Object[] getCells(Row row, String[] sheetHeaders, String[] headersToGet) {
		int colLength = row.getLastCellNum();
		return getCells(row, sheetHeaders, headersToGet, colLength);
	}
	
	private Object[] getCells(Row row, String[] sheetHeaders, String[] headersToGet, int colLength) {
		List<Object> cols = new Vector<Object>();
		for(int colIndex = colStarter; colIndex < colLength; colIndex++) {
			String header = sheetHeaders[colIndex];
			if(ArrayUtilityMethods.arrayContainsValue(headersToGet, header)) {
				Cell thisCell = row.getCell(colIndex);
				cols.add(getCell(thisCell));
			}
		}
		return cols.toArray(new Object[]{});
	}	
	
	public int[] getHeaderIndicies(String sheetName, String[] headers) {
		String[] sheetHeaders = this.clean_headers.get(sheetName);
		
		int numHeadersToGet = headers.length;
		int[] indicesToGet = new int[numHeadersToGet];
		for(int colIdx = 0; colIdx < numHeadersToGet; colIdx++) {
			String headerToGet = headers[colIdx];
			// find the index in sheet headers to return
			indicesToGet[colIdx] = ArrayUtilityMethods.arrayContainsValueAtIndex(sheetHeaders, headerToGet);
		}
		
		return indicesToGet;
	}
	
	public Object[] getNextRow(String sheetName, int[] headerIndicesToGet) {
		Sheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		Object[] thisRow = null;
		while(thisRow == null && counter <= thisSheet.getLastRowNum()) {			
			thisRow = getCells(thisSheet.getRow(counter), headerIndicesToGet);
			counter++;
		}
		
		// set counter back
		sheetCounter.put(sheetName, counter);
		
		// assimilate the properties
		// TODO: this logic isn't valid since we get the headers on instantiation of the instance
//		if(counter == 0) {
//			for(int colIndex = colStarter; colIndex < thisRow.length; colIndex++) {
//				putProp(thisRow[colIndex], sheetName);
//			}
//		}
		
		return thisRow;
	}
	
	private Object[] getCells(Row row, int[] headerIndicesToGet) {
		int numCols = headerIndicesToGet.length;
		List<Object> cols = new Vector<Object>();
		for(int colIndex = colStarter; colIndex < numCols; colIndex++) {
			Cell thisCell = row.getCell(headerIndicesToGet[colIndex]);
			cols.add(getCell(thisCell));
		}
		return cols.toArray(new Object[]{});
	}
	
	/////////////////// END SPECIFIC HEADERS NEXT ROWS ///////////////////

	private Object getCell(Cell thisCell) {
		if(thisCell == null) {
			return "";
		}
		CellType type = thisCell.getCellType();
		if(type == CellType.BLANK) {
			return "";
		}
		if(type == CellType.STRING) {
			return thisCell.getStringCellValue();
		} else if(type == CellType.NUMERIC) {
			if(DateUtil.isCellDateFormatted(thisCell)) {
				return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
			}
			return thisCell.getNumericCellValue();
		} else if(type == CellType.BOOLEAN) {
			return thisCell.getBooleanCellValue() + "";
		} else if(type == CellType.FORMULA) {
			// do the same for the formula value
			CellType formulatype = thisCell.getCachedFormulaResultType();
			if(formulatype == CellType.BLANK) {
				return "";
			}
			if(formulatype == CellType.STRING) {
				return thisCell.getStringCellValue();
			} else if(formulatype == CellType.NUMERIC) {
				if(DateUtil.isCellDateFormatted(thisCell)) {
					return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
				}
				return thisCell.getNumericCellValue();
			} else if(formulatype == CellType.BOOLEAN) {
				return thisCell.getBooleanCellValue();
			}
		}
		return "";
	}
	
	/**
	 * Predict the type via casting
	 * @param value
	 * @return
	 */
	private static SemossDataType getTypeByCast(Object value) {
		if(value instanceof String) {
			return SemossDataType.STRING;
		} else if(value instanceof Number) {
			// check if actually a number
			if( ((Number) value).doubleValue() == Math.rint(((Number) value).doubleValue()) ) {
				return SemossDataType.INT;
			}
			return SemossDataType.DOUBLE;
		} else if(value instanceof SemossDate) {
			// not a perfect check by any means
			// but quick and easy to do
			if(hasTime( ((SemossDate) value).getDate() )) {
				return SemossDataType.TIMESTAMP;
			} else {
				return SemossDataType.DATE;
			}
		} else if(value instanceof Boolean) {
			return SemossDataType.BOOLEAN;
		}
		
		return SemossDataType.STRING;
	}
	
    /**
     * Determines whether or not a date has any time values.
     * @param date The date.
     * @return true iff the date is not null and any of the date's hour, minute,
     * seconds or millisecond values are greater than zero.
     */
    private static boolean hasTime(Date date) {
        if (date == null) {
            return false;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        if (c.get(Calendar.HOUR_OF_DAY) > 0) {
            return true;
        }
        if (c.get(Calendar.MINUTE) > 0) {
            return true;
        }
        if (c.get(Calendar.SECOND) > 0) {
            return true;
        }
        if (c.get(Calendar.MILLISECOND) > 0) {
            return true;
        }
        return false;
    } 

	public Object[][] predictTypes(String sheetName) {
		Sheet lSheet = sheetNames.get(sheetName);
		int numRows = lSheet.getLastRowNum() + 1;

		Row header = lSheet.getRow(0);
		int numCols = header.getLastCellNum();

		Object[][] predictedTypes = new Object[numCols][3];
		List<Map<String, Integer>> additionalFormatTracker = new Vector<Map<String, Integer>>(numCols);

		// Loop through cols, and up to 1000 rows
		for(int colIndex = colStarter; colIndex < numCols; colIndex++) {
			predictTypesLoop(lSheet, numRows, predictedTypes, additionalFormatTracker, colIndex);
		}

		// need to reset all the parses
		reset();
		return predictedTypes;
	}
	
    public Object[][] predictTypes(String sheetName, int[] headersToSelect) {
    	Sheet lSheet = sheetNames.get(sheetName);
    	int numRows = lSheet.getLastRowNum() + 1;
    	int numCols = headersToSelect.length;

    	Object[][] predictedTypes = new Object[numCols][3];
    	List<Map<String, Integer>> additionalFormatTracker = new Vector<Map<String, Integer>>(numCols);

    	// Loop through cols, and up to 1000 rows
    	for(int cellHeader = colStarter; cellHeader < numCols; cellHeader++) {
    		int colIndex = headersToSelect[cellHeader];
    		predictTypesLoop(lSheet, numRows, predictedTypes, additionalFormatTracker, colIndex);
    	}

    	// need to reset all the parses
    	reset();
    	return predictedTypes;
    }

	private void predictTypesLoop(Sheet lSheet, int numRows, Object[][] predictedTypes, List<Map<String, Integer>> additionalFormatTracker, int colIndex) {
		SemossDataType type = null;
		Map<String, Integer> formatTracker = new HashMap<String, Integer>();
		additionalFormatTracker.add(colIndex, formatTracker);
		
		ROW_LOOP : for(int j = 1; j < numRows && j < NUM_ROWS_TO_PREDICT_TYPES; j++) {
			Row row = lSheet.getRow(j);
			if(row != null) {
				Object value = getCell(row.getCell(colIndex));
				if(value instanceof String && value.toString().isEmpty()) {
					continue ROW_LOOP;
				}
				
				SemossDataType newTypePrediction = getTypeByCast(value);
				String additionalFormatting = null;
				if(value instanceof SemossDate) {
					additionalFormatting = ((SemossDate) value).getPattern();
				}
				
				// handle the additional formatting
				if(additionalFormatting != null) {
					if(formatTracker.containsKey(additionalFormatting)) {
						// increase counter by 1
						formatTracker.put(additionalFormatting, new Integer(formatTracker.get(additionalFormatting) + 1));
					} else {
						formatTracker.put(additionalFormatting, new Integer(1));
					}
				}
				
				// if we hit a string
				// we are done
				if(newTypePrediction == SemossDataType.STRING || newTypePrediction == SemossDataType.BOOLEAN) {
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = SemossDataType.STRING;
					predictedTypes[colIndex] = columnPrediction;
					break ROW_LOOP;
				}
				
				if(type == null) {
					// this is the first time we go through
					// just set the type and we are done
					// we only need to go through when we hit a difference
					type = newTypePrediction;
					continue;
				}
				
				if(type == newTypePrediction) {
					// well, nothing for us to do if its the same
					// again, we handle additional formatting
					// at the top
					continue;
				}
				
				// if we hit an integer
				else if(newTypePrediction == SemossDataType.INT) {
					if(type == SemossDataType.DOUBLE) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a double
				else if(newTypePrediction == SemossDataType.DOUBLE) {
					if(type == SemossDataType.INT) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a date
				else if(newTypePrediction == SemossDataType.DATE) {
					if(type == SemossDataType.TIMESTAMP) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a timestamp
				else if(newTypePrediction == SemossDataType.TIMESTAMP) {
					if(type == SemossDataType.DATE) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
			}
		}

		// if an entire column is empty, type will be null
		// why someone has a csv file with an empty column, i do not know...
		if(type == null) {
			type = SemossDataType.STRING;
		}

		// if format tracking is empty
		// just add the type to the matrix
		// and continue
		if(formatTracker.isEmpty()) {
			Object[] columnPrediction = new Object[2];
			columnPrediction[0] = type;
			predictedTypes[colIndex] = columnPrediction;
		} else {
			// format tracker is not empty
			// need to figure out the date situation
			if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				Object[] results = FileHelperUtil.determineDateFormatting(type, formatTracker);
				predictedTypes[colIndex] = results;
			} else {
				// UGH... how did you get here if you are not a date???
				Object[] columnPrediction = new Object[2];
				columnPrediction[0] = type;
				predictedTypes[colIndex] = columnPrediction;
			}
		}
	}
	
	public Map<String, Map<String, String>> getChangedHeaders() {
		Map<String, Map<String, String>> changedHeaders = new Hashtable<String, Map<String, String>>();
		
		// loop through all the sheets
		String[] sheetNames = this.getTables();
		for(String sheetName : sheetNames) {
			// get all the original headers in the sheet
			String[] originalHeaders = original_headers.get(sheetName);
			// get all the new headers in the sheet
			String[] newHeaders = clean_headers.get(sheetName);

			Hashtable<String, String> modHeaders = new Hashtable<String, String>();
			
			// iterate thorugh the headers lists and see what values have changed
			int numCols = newHeaders.length;
			for(int colIdx = 0; colIdx < numCols; colIdx++) {
				String origHeader = originalHeaders[colIdx];
				String newHeader = newHeaders[colIdx];
				
				if(!origHeader.equalsIgnoreCase(newHeader)) {
					modHeaders.put(newHeader, "Original Header Value = " + origHeader);
				}
			}
			
			changedHeaders.put(sheetName, modHeaders);
		}
		
		return changedHeaders;
	}
	
	
	/**
	 * Each sheet becomes a table
	 * @return
	 */
	public String [] getTables() {
		String[] sheets = new String[sheetNames.size()];
		int counter = 0;
		for(String sheet : sheetNames.keySet()) {
			sheets[counter] = sheet;
			counter++;
		}
		
		return sheets;
	}

	/**
	 * 
	 * @param propertyName
	 * @param sheetName
	 */
	private void putProp(String propertyName, String sheetName)
	{
		System.out.println(sheetName + " <>" + propertyName);
		
		Vector<String> propValue = null;
		if(allProps.containsKey(propertyName)) {
			propValue = allProps.get(propertyName);
		} else {
			propValue = new Vector<String>();
		}
		
		if(!propValue.contains(sheetName)) {
			propValue.add(sheetName);
			allProps.put(propertyName, propValue);
		}
	}
	
	/**
	 * Determine the relationships that should exist based on each sheet and its headers
	 * @return
	 */
	public Vector<String> getRelations()
	{
		Vector <String> retVec = new Vector<String>();
		// for each property
		// get the sheetnames and tag them together
		Iterator<String> propKeys = allProps.keySet().iterator();
		
		while(propKeys.hasNext())
		{
			String thisProp = propKeys.next();
			Vector <String> sheets = allProps.get(thisProp);
			
			for(int thisSheetIndex = 0;thisSheetIndex < sheets.size();thisSheetIndex++)
			{
				String curSheet = sheets.remove(0);
				for(int otherSheetIndex = 0; otherSheetIndex < sheets.size(); otherSheetIndex++)
				{
					String fkString = curSheet + "." + thisProp + "." + sheets.get(otherSheetIndex) + "." + thisProp;
					retVec.add(fkString); // it is not what you are thinking
				}
			}
		}
		return retVec;
	}
	
	/**
	 * Reset to start the parser from the beginning of the file
	 */
	public void reset() {
		clear();
		createParser();
	}
	
	/**
	 * Clears the parser and requires you to start the parsing from scratch	
	 */
	public void clear() {
		try {
			if(sourceFile != null) {
				sourceFile.close(); 
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void modifyCleanedHeaders(Map<String, Map<String, String>> excelHeaderNames) {
		for(String sheetName : excelHeaderNames.keySet()) {
			// get existing headers for this sheet
			String[] cleanHeaders = this.clean_headers.get(sheetName);
			
			// get the user changes
			Map<String, String> thisSheetHeaderChanges = excelHeaderNames.get(sheetName);
			
			// iterate through all sets of oldHeader -> newHeader
			for(String oldHeader : thisSheetHeaderChanges.keySet()) {
				String desiredNewHeaderValue = thisSheetHeaderChanges.get(oldHeader);
				// since the user may not want all the headers, we only check if new headers are valid
				// based on the headers they want
				// thus, we need to check and see if the newHeaderValue is actually already used
				int newNameIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(cleanHeaders, desiredNewHeaderValue);
				if(newNameIndex >= 0) {
					// this new header exists
					// lets modify it
					cleanHeaders[newNameIndex] = "NOT_USED_COLUMN_1234567890";
				}
				
				// now we modify what was the old header to be the new header
				int oldHeaderIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(cleanHeaders, oldHeader);
				cleanHeaders[oldHeaderIndex] = desiredNewHeaderValue;
			}
		}
	}
	
	public String[] orderHeadersToGet(String sheetName, String[] headersToGet) {
		String[] currHeaders = clean_headers.get(sheetName);
		List<String> orderedHeaders = new Vector<String>();
		for(String header : currHeaders) {
			if(ArrayUtilityMethods.arrayContainsValue(headersToGet, header)) {
				orderedHeaders.add(header);
			}
		}
		
		return orderedHeaders.toArray(new String[]{});
	}

	public String getFileLocation() {
		return this.fileLocation;
	}
	
	/**
	 * Get the index for a given sheet
	 * @param sheetName
	 * @return
	 */
	public int getSheetIndex(String sheetName) {
		return this.workbook.getSheetIndex(sheetName);
	}
	
	
	///// TESTING CODE STARTS HERE /////

//	public static void main(String [] args) throws Exception
//	{
//		// ugh, need to load this in for the header exceptions
//		// this contains all the sql reserved words
//		TestUtilityMethods.loadDIHelper();
//		
//		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
//		long before, after;
//		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
//		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
//		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/XLUploadTester.xlsx";
//		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Medical_devices_data.xlsx";
//		
//		fileName = "C:/Users/mahkhalil/Desktop/Copy of eXAMPLE_DATA.xlsx";
//		before = System.nanoTime();
//		XLFileHelper test = new XLFileHelper();
//		test.parse(fileName);
//		String [] tables = test.getTables();
//		test.printRow(tables);
//		for(int tabIndex = 0;tabIndex < tables.length;tabIndex++)
//		{
//			test.printRow(test.getHeaders(tables[tabIndex]));
//			test.printRow(test.getNextRow(tables[tabIndex]));
//			test.printRow(test.getNextRow(tables[tabIndex]));
//			test.printRow(test.getNextRow(tables[tabIndex]));
//			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
//			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
//			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
//		}
//		Vector <String> allRels = test.getRelations();
//		
//		for(int tabIndex = 0;tabIndex < allRels.size();tabIndex++)
//		{
//			System.out.println(allRels.elementAt(tabIndex));
//		}
//		
//		//test.printRow(test.getRow());
//		after = System.nanoTime();
//		System.out.println((after - before)/1000000);
//	}

	private void printRow(Object[] nextRow) {
		if(nextRow != null) {
			System.out.println(Arrays.toString(nextRow));
		}
	}
}
