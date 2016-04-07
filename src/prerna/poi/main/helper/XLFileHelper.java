package prerna.poi.main.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import cern.colt.Arrays;
import prerna.util.Utility;

public class XLFileHelper {
	
	int colStarter = 0;

	private	XSSFWorkbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;

	private Map <String, XSSFSheet> sheetNames = new Hashtable<String, XSSFSheet>();
	private Map <String, String[]> headers = new Hashtable<String, String[]>();
	private Map <String, Integer> sheetCounter = new Hashtable<String, Integer>();
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
				
				String[] sheetHeaders = getCells(sheet.getRow(0));
				headers.put(nameOfSheet, sheetHeaders);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String[] getHeaders(String sheetName) {
		return headers.get(sheetName);
	}
	
	public String[] getNextRow(String sheetName) {
		XSSFSheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 0;
		if(sheetCounter.containsKey(sheetName)) {
			counter = sheetCounter.get(sheetName);
		}
		
		String [] thisRow = null;
		if(counter < thisSheet.getLastRowNum()) {
			thisRow = getCells(thisSheet.getRow(counter));
		}
		
		// assimilate the properties
		if(counter == 0)
		{
			for(int colIndex = colStarter;colIndex < thisRow.length;colIndex++) {
				putProp(thisRow[colIndex], sheetName);
			}
		}
		
		// set counter back
		counter++;		
		sheetCounter.put(sheetName, counter);

		return thisRow;
	}
	
	private String[] getCells(XSSFRow row) {
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
	

	private String getCell(XSSFCell thisCell) {
		if(thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK) {
			if(thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			}
			else if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
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
		for(int i = colStarter; i < numCells; i++) {
			String type = null;
			ROW_LOOP : for(int j = 1; j < numRows; j++) {
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
				types[i] = "varchar(255)";
			} else {
				types[i] = type;
			}
		}

		// need to reset all the parses
		reset();
		return types;
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
	
	
	///// TESTING CODE STARTS HERE /////

	public static void main(String [] args) throws Exception
	{
		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		long before, after;
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/XLUploadTester.xlsx";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Medical_devices_data.xlsx";
		before = System.nanoTime();
		XLFileHelper test = new XLFileHelper();
		test.parse(fileName);
		String [] tables = test.getTables();
		for(int tabIndex = 0;tabIndex < tables.length;tabIndex++)
		{
			test.printRow(test.getNextRow(tables[tabIndex]));
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
		System.out.println(Arrays.toString(nextRow));
	}

}
