package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileReader;
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

import prerna.engine.api.IEngine;
import prerna.util.Utility;


public class XLFileHelper {
	
	int colStarter = 0;
	
	FileInputStream sourceFile = null;
	String fileName = null;
	
	char delimiter = ',';
	String [] curHeaders = null;
	Hashtable <String, String> cleanDirtyMapper = new Hashtable<String, String>();
	Hashtable <String, String> dirtyTypeMapper = new Hashtable<String, String>();
	IEngine engine = null;

	private static final Logger logger = LogManager.getLogger(POIReader.class.getName());
		
	private Hashtable <String, XSSFSheet> sheetNames = new Hashtable<String, XSSFSheet>();
	private Hashtable <String, Integer> sheetCounter = new Hashtable<String, Integer>();
	private Hashtable <String, Vector<String>> allProps = new Hashtable <String, Vector<String>> ();
	private	XSSFWorkbook workbook = null;


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
			test.printRow(test.getRow(tables[tabIndex]));
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

	// FROM CSV Helper
	
	public void parse(String fileName)
	{
		this.fileName = fileName;
		createParser();
	}

	private void createParser()
	{
		try {
			sourceFile = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(sourceFile);
			
			// get all the sheets
			int totalSheets = workbook.getNumberOfSheets();
			
			// I am not sure if I should assimilate the sheets
			for(int sheetIndex = 0;sheetIndex < totalSheets;sheetIndex++)
			{
				String nameOfSheet = workbook.getSheetAt(sheetIndex).getSheetName();
				sheetNames.put(nameOfSheet, workbook.getSheet(nameOfSheet));
			}
			/*
				assimilateSheet(workbook.getSheetAt(sheetIndex).getSheetName(), workbook);
			
			// load the Loader tab to determine which sheets to load
			// the next thing is to look at relation ships and add the appropriate column that I want to the respective concepts

			System.out.println("Lucky !!" + concepts + " <> " + relations);
			System.out.println("Properties" + allProps);
			
			// this call 
			cleanProps();
			
			System.out.println("Ok.. now what ?");
			synchronizeRelations();
			// the next thing is to find
			// which of these have fully formed tables vs. which of these are just reference data
			// and then proceed to insert it appropriately
			findRelations();
			

			// now I need to create the tables
			Enumeration <String> conceptKeys = concepts.keys();
			while(conceptKeys.hasMoreElements())
			{
				String thisConcept = conceptKeys.nextElement();
				createTable(thisConcept);
				processTable(thisConcept, workbook);
			}
			// before I process this.. I need to process all the interim tables
			Enumeration <String> interimKeys = interimConcepts.keys();
			while(interimKeys.hasMoreElements())
			{
				String thisConcept = interimKeys.nextElement();
				processInterimTable(thisConcept, workbook);
			}
			
			// I need to first create all the concepts
			// then all the relationships
			Enumeration <String> relationConcepts = relations.keys();
			while(relationConcepts.hasMoreElements())
			{
				String thisConcept = relationConcepts.nextElement();
				Vector <String> allRels = relations.get(thisConcept);

				//for(int toIndex = 0;toIndex < allRels.size();toIndex++)
					// now process each one of these things
					//createRelations(thisConcept, allRels.elementAt(toIndex), workbook);
			}

			*/
			
		}catch(Exception ex)
		{
			
		}
	}
		
	

	public String[] getRow(String sheetName)
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
		
	public void printRow(String [] data)
	{
		for(int dataIndex = 0;dataIndex < data.length;dataIndex++)
			System.out.print("["+data[dataIndex] + "]");
		
		System.out.println("----");
	}

	public void setDelimiter(char charAt) {
		// TODO Auto-generated method stub
		this.delimiter = charAt;
	}
}
