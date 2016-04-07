package prerna.poi.main.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import cern.colt.Arrays;
import prerna.util.Utility;

public class XLFileHelper {
	
	private static final Logger LOGGER = LogManager.getLogger(XLFileHelper.class.getName());

	int colStarter = 0;
	
	private	XSSFWorkbook workbook = null;
	private FileInputStream sourceFile = null;
	private String fileName = null;

	private Hashtable <String, XSSFSheet> sheetNames = new Hashtable<String, XSSFSheet>();
	private Hashtable <String, Integer> sheetCounter = new Hashtable<String, Integer>();
	private Hashtable <String, Vector<String>> allProps = new Hashtable <String, Vector<String>> ();

	public void parse(String fileName) {
		this.fileName = fileName;
		createParser();
	}
	
	public String [] getTables()
	{
		String [] sheets = new String[sheetNames.size()];
		Enumeration <String> sheetKeys = sheetNames.keys();
		
		
		for(int sheetIndex = 0;sheetKeys.hasMoreElements();sheets[sheetIndex] = sheetKeys.nextElement(),sheetIndex++);
		
		return sheets;
	}

	private void putProp(String propertyName, String sheetName)
	{
		System.out.println(sheetName + " <>" + propertyName);
		Vector <String> propValue = new Vector<String>();
		if(allProps.containsKey(propertyName))
			propValue = allProps.get(propertyName);
		if(!propValue.contains(sheetName))
		{
			propValue.add(sheetName);
			allProps.put(propertyName, propValue);
		}
	}
	
	public Vector<String> getRelations()
	{
		Vector <String> retString = new Vector<String>();
		// for each property
		// get the sheetnames and tag them together
		Enumeration <String> propKeys = allProps.keys();
		
		while(propKeys.hasMoreElements())
		{
			String thisProp = propKeys.nextElement();
			
			Vector <String> sheets = allProps.get(thisProp);
			
			for(int thisSheetIndex = 0;thisSheetIndex < sheets.size();thisSheetIndex++)
			{
				String curSheet = sheets.remove(0);
				for(int otherSheetIndex = 0;otherSheetIndex < sheets.size();otherSheetIndex++)
				{
					String fkString = curSheet + "." + thisProp + "." + sheets.get(otherSheetIndex) + "." + thisProp;
					retString.add(fkString); // it is not what you are thinking
				}
			}
		}
		return retString;
	}
	
	public String[] getCells(XSSFRow row)
	{
		int colLength = row.getLastCellNum();
		return getCells(row, colLength);
	}

	public String[] getCells(XSSFRow row, int totalCol)
	{
		int colLength = totalCol;
		String [] cols = new String[colLength];
		for(int colIndex = colStarter;colIndex < colLength;colIndex++)
		{
			XSSFCell thisCell = row.getCell(colIndex);
			// get all of this into a string
			if(thisCell != null && row.getCell(colIndex).getCellType() != Cell.CELL_TYPE_BLANK)
			{
				if(thisCell.getCellType() == Cell.CELL_TYPE_STRING)
				{
					cols[colIndex] = thisCell.getStringCellValue();
					cols[colIndex] = Utility.cleanString(cols[colIndex], true);
				}
				if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC)
					cols[colIndex] = "" + thisCell.getNumericCellValue();
			}
			else
			{
				cols[colIndex] = "";
			}
		}	

		return cols;
	}
	

	public String getCell(XSSFCell thisCell) {
		if(thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK)
		{
			if(thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			}
			else if(thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
			}
		}
		return "";
	}

	private void createParser()
	{
		try {
			sourceFile = new FileInputStream(fileName);
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

	

	public String[] getNextRow(String sheetName)
	{
		XSSFSheet thisSheet = sheetNames.get(sheetName);
		
		int counter = 0;
		if(sheetCounter.containsKey(sheetName))
			counter = sheetCounter.get(sheetName);
		
		String [] thisRow = null;
		if(counter < thisSheet.getLastRowNum())
			thisRow = getCells(thisSheet.getRow(counter));
		
		// assimilate the properties
		if(counter == 0)
		{
			for(int colIndex = colStarter;colIndex < thisRow.length;colIndex++)
				putProp(thisRow[colIndex], sheetName);
		}
		
		// set counter back
		counter++;		
		sheetCounter.put(sheetName, counter);

		return thisRow;
	}

	// resets everything.. just something to watch
	public void reset()
	{
		try {
			sourceFile.close();
			createParser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reset(boolean getHeader)
	{
		reset();
	}

	public String[] getSheets() {
		return null;
	}
	
	
	/**
	 * Used for testing
	 * @param args
	 * @throws Exception
	 */
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
