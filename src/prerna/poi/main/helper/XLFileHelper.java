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

public class XLFileHelper {
	
	int colStarter = 0;

	private	XSSFWorkbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileLocation = null;

	private Map <String, XSSFSheet> sheetNames = new Hashtable<String, XSSFSheet>();
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
				String nameOfSheet = workbook.getSheetAt(sheetIndex).getSheetName();
				sheetNames.put(nameOfSheet, workbook.getSheet(nameOfSheet));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	
	/**
	 * Each sheet becomes a table
	 * @return
	 */
	public String [] getTables() {
		String[] sheets = new String[sheetNames.size()];
		int counter = 0;
		for(String sheet : sheets) {
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
