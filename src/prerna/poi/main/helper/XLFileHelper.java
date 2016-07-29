package prerna.poi.main.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import cern.colt.Arrays;
import prerna.poi.main.HeadersException;
import prerna.test.TestUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class XLFileHelper {
	
	int colStarter = 0;
	
	private static final int NUM_ROWS_TO_PREDICT_TYPES = 1000;

	private	XSSFWorkbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;

	// contains the string to the excel sheet object
	private Map <String, XSSFSheet> sheetNames = new Hashtable<String, XSSFSheet>();
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
			workbook = new XSSFWorkbook(sourceFile);

			// get all the sheets
			int totalSheets = workbook.getNumberOfSheets();

			// store all the sheets
			for(int sheetIndex = 0;sheetIndex < totalSheets; sheetIndex++) {
				XSSFSheet sheet = workbook.getSheetAt(sheetIndex);
				String nameOfSheet = sheet.getSheetName();
				sheetNames.put(nameOfSheet, sheet);
				
				String[] sheetHeaders = getSheetHeaders(sheet);
				if(sheetHeaders == null) {
					sheetHeaders = new String[]{};
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
	public String[] getSheetHeaders(XSSFSheet sheet) {
		int counter = 0;
		XSSFRow headerRow = null;
		while(headerRow == null && counter < sheet.getLastRowNum()) {
			headerRow = sheet.getRow(counter);
			counter++;
		}
		
		// get the headers
		String[] sheetHeaders = getCells(headerRow);
		// set the new start for the getNextRow for this sheet
		sheetCounter.put(sheet.getSheetName(), counter);
		
		return sheetHeaders;
	}
	
	/////////////////// START ALL NEXT ROWS ///////////////////
	
	public String[] getNextRow(String sheetName) {
		XSSFSheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		String [] thisRow = null;
		while(thisRow == null && counter < thisSheet.getLastRowNum()) {
			thisRow = getCells(thisSheet.getRow(counter));
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
	
	private String[] getCells(XSSFRow row) {
		if(row == null) {
			return null;
		}
		int colLength = row.getLastCellNum();
		return getCells(row, colLength);
	}
	
	private String[] getCells(XSSFRow row, int totalCol)
	{
		int colLength = totalCol;
		String [] cols = new String[colLength];
		for(int colIndex = colStarter; colIndex < colLength; colIndex++) {
			XSSFCell thisCell = row.getCell(colIndex);
			cols[colIndex] = getCell(thisCell);
		}	

		return cols;
	}
	
	/////////////////// END ALL NEXT ROWS ///////////////////

	/////////////////// START SPECIFIC HEADERS NEXT ROWS ///////////////////

	public String[] getNextRow(String sheetName, String[] headersToGet) {
		String[] allHeaders = clean_headers.get(sheetName);
		if(allHeaders.length == headersToGet.length) {
			return getNextRow(sheetName);
		}
		
		XSSFSheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		String [] thisRow = null;
		while(thisRow == null && counter < thisSheet.getLastRowNum()) {
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
	
	private String[] getCells(XSSFRow row, String[] sheetHeaders, String[] headersToGet) {
		int colLength = row.getLastCellNum();
		return getCells(row, sheetHeaders, headersToGet, colLength);
	}
	
	private String[] getCells(XSSFRow row, String[] sheetHeaders, String[] headersToGet, int colLength) {
		List<String> cols = new Vector<String>();
		for(int colIndex = colStarter; colIndex < colLength; colIndex++) {
			String header = sheetHeaders[colIndex];
			if(ArrayUtilityMethods.arrayContainsValue(headersToGet, header)) {
				XSSFCell thisCell = row.getCell(colIndex);
				cols.add(getCell(thisCell));
			}
		}
		return cols.toArray(new String[]{});
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
	
	public String[] getNextRow(String sheetName, int[] headerIndicesToGet) {
		XSSFSheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 1;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		String [] thisRow = null;
		while(thisRow == null && counter < thisSheet.getLastRowNum()) {			
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
	
	private String[] getCells(XSSFRow row, int[] headerIndicesToGet) {
		int numCols = headerIndicesToGet.length;
		List<String> cols = new Vector<String>();
		for(int colIndex = colStarter; colIndex < numCols; colIndex++) {
			XSSFCell thisCell = row.getCell(colIndex);
			cols.add(getCell(thisCell));
		}
		return cols.toArray(new String[]{});
	}
	
	/////////////////// END SPECIFIC HEADERS NEXT ROWS ///////////////////

	private String getCell(XSSFCell thisCell) {
		if(thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK) {
			if(thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			} else if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
			} else if(thisCell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
				return thisCell.getBooleanCellValue() + "";
			}
		}
		return "";
	}
	
	public String[] predictRowTypes(String sheetName) {
		XSSFSheet lSheet = sheetNames.get(sheetName);
		int numRows = lSheet.getLastRowNum();
		
		XSSFRow header = lSheet.getRow(0);
		int numCells = header.getLastCellNum();
		
		String [] types = new String[numCells];
		// Loop through cols, and up to 1000 rows
		for(int i = colStarter; i < numCells; i++) {
			String type = null;
			ROW_LOOP : for(int j = 1; j < numRows && j < NUM_ROWS_TO_PREDICT_TYPES; j++) {
				XSSFRow row = lSheet.getRow(j);
				if(row != null) {
					XSSFCell cell = row.getCell(i);
					if(cell != null) {
						String val = getCell(cell);
						if(val.isEmpty()) {
							continue ROW_LOOP;
						}
						String newTypePred = (Utility.findTypes(val)[0] + "").toUpperCase();
						if(newTypePred.contains("VARCHAR")) {
							type = newTypePred;
							break ROW_LOOP;
						}
						
						// need to also add the type null check for the first row
						if(!newTypePred.equals(type) && type != null) {
							// this means there are multiple types in one column
							// assume it is a string 
							if( (type.equals("INT") || type.equals("DOUBLE")) && (newTypePred.equals("INT") || 
									newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ) {
								// for simplicity, make it a double and call it a day
								// TODO: see if we want to impl the logic to choose the greater of the newest
								// this would require more checks though
								type = "DOUBLE";
							} else {
								// should only enter here when there are numbers and dates
								// TODO: need to figure out what to handle this case
								// for now, making assumption to put it as a string
								type = "VARCHAR(800)";
								break ROW_LOOP;
							}
						} else {
							// type is the same as the new predicated type
							// or type is null on first iteration
							type = newTypePred;
						}
					}
				}
			}
			if(type == null) {
				// no data for column....
				types[i] = "VARCHAR(255)";
			} else {
				types[i] = type;
			}
		}

		// need to reset all the parses
		reset();
		return types;
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
	
	
	///// TESTING CODE STARTS HERE /////

	public static void main(String [] args) throws Exception
	{
		// ugh, need to load this in for the header exceptions
		// this contains all the sql reserved words
		TestUtilityMethods.loadDIHelper();
		
		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		long before, after;
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/XLUploadTester.xlsx";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Medical_devices_data.xlsx";
		
		fileName = "C:/Users/mahkhalil/Desktop/Copy of eXAMPLE_DATA.xlsx";
		before = System.nanoTime();
		XLFileHelper test = new XLFileHelper();
		test.parse(fileName);
		String [] tables = test.getTables();
		test.printRow(tables);
		for(int tabIndex = 0;tabIndex < tables.length;tabIndex++)
		{
			test.printRow(test.getHeaders(tables[tabIndex]));
			test.printRow(test.getNextRow(tables[tabIndex]));
			test.printRow(test.getNextRow(tables[tabIndex]));
			test.printRow(test.getNextRow(tables[tabIndex]));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
		}
		Vector <String> allRels = test.getRelations();
		
		for(int tabIndex = 0;tabIndex < allRels.size();tabIndex++)
		{
			System.out.println(allRels.elementAt(tabIndex));
		}
		
		//test.printRow(test.getRow());
		after = System.nanoTime();
		System.out.println((after - before)/1000000);
	}

	private void printRow(String[] nextRow) {
		if(nextRow != null) {
			System.out.println(Arrays.toString(nextRow));
		}
	}
}
