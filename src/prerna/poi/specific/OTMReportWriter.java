/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/


package prerna.poi.specific;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

/** 
 * This class is responsible for outputting the OTMReport to an excel files. The OTM report details LPI systems, interfaces,
 *  waves, and data sources. The out put is into excel over a series of worksheets under the workbook name of OTMReport.
 *  
 *  Author: Joseph Vidalis  
 *  Email:  jvidalis@deloitte.com
 */
public class OTMReportWriter {

	private static final Logger logger = LogManager.getLogger(OTMReportWriter.class);

	private static final String STACKTRACE = "StackTrace: ";
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private XSSFWorkbook wb;
	private static String fileLoc = "";
	private static String templateLoc = "";

	public Hashtable<String,XSSFCellStyle> myStyles;

	public OTMReportWriter(){
	}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used from a template workbook.
	 *   If no template workbook exists, a blank workbook is created.
	 * @param systemName		String containing the name of the system to create the report for
	 * @param templateName		Name of the template file to use for the new workbook
	 */
	public void makeWorkbook(String systemName, String templateName)
	{
		
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports"+ DIR_SEPARATOR + systemName;
		templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "export" + DIR_SEPARATOR + "Reports" + DIR_SEPARATOR + templateName;//"Individual_System_Transition_Report_Template.xlsx";

		wb =new XSSFWorkbook();

		//if a report template exists, then create a copy of the template, otherwise create a new workbook
		if(templateLoc!=null)
			try{
				wb = (XSSFWorkbook)WorkbookFactory.create(new File(templateLoc));
			}
		catch(Exception e){
			wb=new XSSFWorkbook();
		}
		else wb=new XSSFWorkbook();
		makeStyles(wb);
	}
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * 
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String systemName)
	{
		makeWorkbook(systemName, null);
	}
	
	/**
	 * Checks to see if writer has a workbook.
	 * 
	 * @return boolean True if System has a workbook
	 */
	public boolean hasWorkbook() {
		return wb != null;
	}

	/**
	 * Writes the workbook to the file location.
	 * 
	 * @return boolean True if write of the workbook is successful
	 */
	public boolean writeWorkbook()
	{
		boolean success = false;
		try{
			Utility.writeWorkbook(wb, fileLoc);
			success = true;
		} catch (Exception ex) {
			success = false;
			logger.error(STACKTRACE, ex);
		}
		return success;
	}

	public static String getFileLoc(){
		return fileLoc;
	}
	
	/**
	 * Writes a generic list sheet from hashtable. A String based sort is applied to the first column of the data.
	 * 
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 * @param c1 		Primary column to sort the list upon
	 * @param c2		Secondary column to sort the list upon
	 */
	public void writeListSheet(String sheetName, HashMap<String,Object> result, int c1, int c2){
		// Grab Sheet to write to
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		
		//Pull the data array from the Hash table
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get(DHMSMTransitionUtility.DATA_KEY);
		
		if( c2 < 0 ){dataList = sortArrayList(dataList, c1); }
		else{dataList = sortArrayList(dataList, c1,c2);	}
				
		// Start upon the first row
		int rowToStart = 3;
		
		for (int row=0; row<dataList.size(); row++) {
			//Grab the array of objects from the arraylist
			Object[] resultRowValues = dataList.get(row);
			
			//Declare the row we are writing to
			XSSFRow rowToWriteOn;
			rowToWriteOn = sheetToWriteOver.createRow(rowToStart+row);

			//For all columns in the row, put the data into each column
			for (int col=0; col < resultRowValues.length; col++) {
				XSSFCell cellToWriteOn;
				cellToWriteOn = rowToWriteOn.createCell(col);
				printToCell(resultRowValues[col], cellToWriteOn);
			}
		}
	}
	
	public void writeListSheet(String sheetName, HashMap<String,Object> result)
	{
		writeListSheet(sheetName, result, 0, 2);
	}
	
	public void writeListSheet(String sheetName, HashMap<String,Object> result, int c1)
	{
		writeListSheet(sheetName, result, c1, -1);
	}
	
	public void writeICDCountSheet( String sheetName, HashMap<String,Object> fHash, HashMap<String,Object> rHash )
	{
		ArrayList<Object[]> fList = (ArrayList<Object[]>)fHash.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> rList = (ArrayList<Object[]>)rHash.get(DHMSMTransitionUtility.DATA_KEY);
		fList = sortArrayList(fList, 1);
		rList = sortArrayList(rList, 1);
		
		ArrayList<Object[]> cList = new ArrayList<>();
		boolean finished = false;
		int f = 0;
		int r = 0;
		
		
		while( !finished )
		{
			if( f == fList.size() && r == rList.size() )
			{
				finished = true;
			}
			else if( f == fList.size() )
			{
				//Add rList Element
				Object[] rRowValues = rList.get(r);
				Object[] cRowValues = new Object[5];
				cRowValues[0] = rRowValues[0];
				cRowValues[1] = rRowValues[1];
				cRowValues[2] = 0.0;
				cRowValues[3] = rRowValues[2];
				cRowValues[4] = "Uni-directional";
				r++;
				cList.add(cRowValues);
			}
			else if( r == rList.size() )
			{
				//Add fList Element
				Object[] fRowValues = fList.get(f);
				Object[] cRowValues = new Object[5];
				cRowValues[0] = fRowValues[0];
				cRowValues[1] = fRowValues[1];
				cRowValues[2] = fRowValues[2];
				cRowValues[3] = 0.0;
				cRowValues[4] = "Uni-directional";
				f++;
				cList.add(cRowValues);
			}
			else
			{
				Object[] rRowValues = rList.get(r);
				Object[] fRowValues = fList.get(f);
				Object[] cRowValues = new Object[5];
				if( ((String)rRowValues[1]).compareToIgnoreCase((String)fRowValues[1]) == 0 ) //Same System
				{
					cRowValues[0] = fRowValues[0];
					cRowValues[1] = fRowValues[1];
					cRowValues[2] = fRowValues[2];
					cRowValues[3] = rRowValues[2];
					cRowValues[4] = "Bi-directional";
					f++;
					r++;
				}
				else if( ((String)rRowValues[1]).compareToIgnoreCase((String)fRowValues[1]) < 0 ) //rlist < flist
				{
					//add rList Element
					cRowValues[0] = rRowValues[0];
					cRowValues[1] = rRowValues[1];
					cRowValues[2] = 0.0;
					cRowValues[3] = rRowValues[2];
					cRowValues[4] = "Uni-directional";
					r++;
				}
				else //String 1 > String 2
				{
					cRowValues[0] = fRowValues[0];
					cRowValues[1] = fRowValues[1];
					cRowValues[2] = fRowValues[2];
					cRowValues[3] = 0.0;
					cRowValues[4] = "Uni-directional";
					f++;
				}
				//Determine which list to add from
				cList.add(cRowValues);
			}
		}
		
		HashMap<String, Object> dataHash = new HashMap<>();
		dataHash.put(DHMSMTransitionUtility.DATA_KEY, cList);

		writeListSheet(sheetName, dataHash, 1);
	}
	
	/**
	 * Writes an excel sheet for System, Description and Owner. A String based sort is applied to the first column of the data. 
	 *   Takes multiple system owners and spreads them across the rows, such that systems and descriptions are distinct in value.
	 *   
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	
	public void writeWaveSheet(String sheetName, HashMap<String,Object> result, HashMap<String,Object> sysInfo){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> sysList = (ArrayList<Object[]>)sysInfo.get(DHMSMTransitionUtility.DATA_KEY);
		dataList = sortArrayList(dataList, 0);
		sysList = sortArrayList(sysList, 0);
		sysList = deleteDuplicates(sysList,0);

		int rowToStart = 3;
		int wave = 255;
		int year = 0;
		int iocYear = 0;
		int quarter = 0;
		int iocQuarter = 0;
		
		XSSFRow rowToWriteOn = null;
		XSSFCell cellToWriteOn = null;

		//Find IOC Wave
		boolean flag = true;
		int x = 0;
		while( flag && x < dataList.size()  )
		{
			Object[] resultRowValues = dataList.get(x);
			if( resultRowValues[1].equals("IOC"))
			{
				iocQuarter = Integer.parseInt((String)resultRowValues[2]);
				iocYear = Integer.parseInt((String)resultRowValues[3]);
				flag = false;
			}
			x++;
		}
		
		
		
		for( int i = 0; i < sysList.size(); i++)//Iterate over the systems
		{
			wave = 255;
			
			rowToWriteOn = sheetToWriteOver.createRow(rowToStart+i);
			
			Object[] resultRowValues = sysList.get(i);
			String systemName = (String)resultRowValues[0];
			int row = findRowFromString(dataList, (String)resultRowValues[0], 0 );
			if( resultRowValues[3].equals("Y"))//Enterprise
			{
				cellToWriteOn = rowToWriteOn.createCell(0);
				printToCell(systemName, cellToWriteOn);
				cellToWriteOn = rowToWriteOn.createCell(1);
				printToCell( "IOC", cellToWriteOn, myStyles.get("rightStyle") );
				cellToWriteOn = rowToWriteOn.createCell(2);
				printToCell(iocQuarter, cellToWriteOn);
				cellToWriteOn = rowToWriteOn.createCell(3);
				printToCell(iocYear, cellToWriteOn);
			}
			else if( row >= 0)
			{
				
				boolean whileFlag = true;
				do
				{
					Object[] waveRowValues = dataList.get(row);
					if( systemName.compareTo((String)waveRowValues[0]) == 0 )// Same System Evaluate Wave Minimum
					{
						if( ((String)waveRowValues[1]).compareTo("IOC") == 0 )
						{
							wave = 0;
							quarter = Integer.parseInt((String)waveRowValues[2]);
							year = Integer.parseInt((String)waveRowValues[3]);
						}
						else if( Integer.parseInt((String)waveRowValues[1]) < wave)
						{
							wave = Integer.parseInt((String)waveRowValues[1]);
							quarter = Integer.parseInt((String)waveRowValues[2]);
							year = Integer.parseInt((String)waveRowValues[3]);
						}
					}else //Not the same system. End Loop
					{
						whileFlag = false;
					}
					row++;
				}while (whileFlag);
				
				// Print out results
				
				cellToWriteOn = rowToWriteOn.createCell(0);
				printToCell(systemName, cellToWriteOn);
				cellToWriteOn = rowToWriteOn.createCell(1);
				
				cellToWriteOn.setCellStyle(myStyles.get("rightStyle"));
				if( wave == 0 )	{ 
					printToCell( "IOC", cellToWriteOn, myStyles.get("rightStyle") );
				}
				else { printToCell( wave, cellToWriteOn); }
				
				cellToWriteOn = rowToWriteOn.createCell(2);
				printToCell(quarter, cellToWriteOn);
				cellToWriteOn = rowToWriteOn.createCell(3);
				printToCell(year, cellToWriteOn);
			}
			else
			{
				cellToWriteOn = rowToWriteOn.createCell(0);
				printToCell(systemName, cellToWriteOn);
				for(int j = 1; j <= 3; j++)
				{
					cellToWriteOn = rowToWriteOn.createCell(j);
					printToCell( "TBD", cellToWriteOn, myStyles.get("rightStyle") );
				}
			}
		}
	}
	
	/**
	 * Writes a generic list sheet from hashtable. A String based sort is applied to the first column of the data. Takes
	 *   multiple system owners and spreads them across the rows, such that systems and descriptions are distinct in value.
	 *   
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	public void writeSystemDataSheet(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get(DHMSMTransitionUtility.DATA_KEY);
		dataList = sortArrayList(dataList, 0);
		String lastSystem = "";
		
		int rowToStart = 3;
		int printRow = 0;
		int colIncrease = 0;
		boolean initial = true;
		XSSFRow rowToWriteOn = null;
		XSSFCell cellToWriteOn = null;
		for (int row = 0; row < dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);

			if (lastSystem.compareToIgnoreCase((String) resultRowValues[0]) != 0) {
				if (colIncrease == 0 && !initial) {
					colIncrease++;
					if (rowToWriteOn != null) {
						cellToWriteOn = rowToWriteOn.createCell(resultRowValues.length - 1 + colIncrease);
					}
					if (cellToWriteOn != null) {
						printToCell("", cellToWriteOn);
					}
				}
				initial = false;
				colIncrease = 0;
				rowToWriteOn = sheetToWriteOver.createRow(rowToStart + printRow);
				for (int col = 0; col < resultRowValues.length; col++) {
					cellToWriteOn = rowToWriteOn.createCell(col);
					if (col == 4 || col == 5) {
						printToCell(resultRowValues[col], cellToWriteOn, myStyles.get("rightStyle"));
					} else if (col != 3) {
						printToCell(resultRowValues[col], cellToWriteOn);
					} else {
						if (((String) resultRowValues[col]).compareTo("Y") == 0) {
							printToCell("Central (Single Instance)", cellToWriteOn);
						} else {
							printToCell("Local (Multiple Instances)", cellToWriteOn);
						}

					}
				}
				printRow++;
				lastSystem = (String) resultRowValues[0];
			} else {
				colIncrease++;
				if (rowToWriteOn != null) {
					cellToWriteOn = rowToWriteOn.createCell(resultRowValues.length - 1 + colIncrease);
				}
				printToCell(resultRowValues[resultRowValues.length - 1], cellToWriteOn);
			}
		}
	}
	
	private ArrayList<Object[]> deleteDuplicates( ArrayList<Object[]> inList, int column )
	{
		ArrayList<Object[]> outList = new ArrayList<>();
		String current = "";
		for( int i = 0; i < inList.size(); i++ )
		{
			Object[] row = inList.get(i);
			if ( current.compareTo((String)row[column]) != 0 )
			{
				outList.add(row);
			}
			current = (String)row[column];
		}
		return outList;
	}
	
	/**
	 * Sorts an array list by a column. Uses a string representation of the value.
	 * @param inList	Array List to be sorted
	 * @param column	Column to sort the data upon
	 * @return outList	ArrayList containing the sorted array list
	 */
	private ArrayList<Object[]> sortArrayList( ArrayList<Object[]> inList, int column )
	{
		ArrayList<Object[]> outList = new ArrayList<>();
		
		//Sorting Loops
		for( int i = 0; i < inList.size(); i++)
		{
			int index = 0;
			Object[] row = inList.get(i);
			
			//TODO Change to while loop to decrease order
			//Iterate over the outList to find proper index for the row
			for( int j = 0; j < i; j++)
			{
				Object[] outRow = outList.get(j);
				//Find index to insert the row into ignore case
				if( ((String)row[column]).toLowerCase().compareTo(((String)outRow[column]).toLowerCase()) > 0)
				{
					index = j + 1;
				}
			}
			//Insert row into the outList
			outList.add(index, row);
		}
		//Warn if size has diminished
		if(outList.size() != inList.size())	{logger.warn("Warning: Data Lost in Single Column Sort");}
		return outList;
	}
	
	/**
	 * Sorts an array list by a column. Uses a string representation of the value. Sorts all entries
	 *  upon the primary column. Then, sorts identical primary columns by a secondary column
	 * @param inList	Array List to be sorted
	 * @param column1	Primary Column to sort the data upon
	 * @param column2	Secondary Column to sort the data upon
	 * @return outList	ArrayList containing the sorted array list
	 */
	private ArrayList<Object[]> sortArrayList( ArrayList<Object[]> inList, int column1, int column2 )
	{
		//Dimension needed array Lists
		ArrayList<Object[]> outList = new ArrayList<>();
		ArrayList<Object[]> primList = null;
		ArrayList<Object[]> secList = new ArrayList<>();
		//Sort the array list by the primary column
		primList = sortArrayList(inList, column1);
		
		String current = "";
		for( int i = 0; i < primList.size(); i++)
		{
			Object[] row = primList.get(i);
			if( ((String)row[column1]).compareTo(current) !=0 )//Different
			{
				if( !secList.isEmpty() )
				{
					//Sort sec Arraylist by Secondary Column
					secList = sortArrayList(secList,column2);
					//Add Elements to outList
					outList.addAll(secList);
					//Clear secList
					secList = new ArrayList<>();
				}
				//Set current primary column value
				current = (String)row[column1];
				//Add row to sec List
				secList.add(row);
			}
			else
			{
				//Add row to secList
				secList.add(row);
			}
		}
		
		//Sort sec Arraylist by Secondary Column
		secList = sortArrayList(secList,column2);
		//Add Final Elements to outList
		outList.addAll(secList);
		
		//Warn if size has diminished
		if(outList.size() != inList.size())	{logger.warn("Warning: Data Lost in Double Column Sort");}
		return outList;
	}
	
	
	/**
	 * Prints the entries of the array list to console. 
	 * @param inList	Array List to be sorted
	 */
	private void printArrayList( ArrayList<Object[]> inList)
	{
		for(int i = 0; i < inList.size(); i++)
		{
			//Pull row from arraylist
			Object[] row = inList.get(i);
			for( int j = 0; j < row.length; j++)
			{
				logger.info(row[j]+ "\t");
			}
			//Add a new line character at the end of a arraylist print
			logger.info("\n");
		}
	}
	
	private boolean hasStringValue( ArrayList<Object[]> aList, String value, int column)
	{
		for( int i = 0; i < aList.size(); i++)
		{
			Object[] row = aList.get(i);
			if( ( (String)row[column] ).compareTo(value) == 0 )
			{
				return true;
			}
		}
		return false;
		
	}
	
	private int findRowFromString( ArrayList<Object[]> aList, String value, int column)
	{
		for( int i = 0; i < aList.size(); i++)
		{
			Object[] row = aList.get(i);
			if( ( (String)row[column] ).compareTo(value) == 0 )
			{
				return i;
			}
		}
		return -1;
		
	}
	
	private void printToCell( Object objectToWrite, XSSFCell cellToWriteOn, XSSFCellStyle style)
	{
		cellToWriteOn.setCellStyle(style);
		if(objectToWrite instanceof Double)
			cellToWriteOn.setCellValue((Double)objectToWrite);
		else if(objectToWrite instanceof Integer)
			cellToWriteOn.setCellValue((Integer)objectToWrite);
		else
			cellToWriteOn.setCellValue(((String)objectToWrite).replaceAll("\"", "").replace("_"," "));	
	}
	
	private void printToCell( Object objectToWrite, XSSFCell cellToWriteOn)
	{
		if(objectToWrite instanceof Double)
		{
			cellToWriteOn.setCellValue((Double)objectToWrite);
			cellToWriteOn.setCellStyle(myStyles.get("rightStyle"));
		}
		else if(objectToWrite instanceof Integer)
		{
			cellToWriteOn.setCellValue((Integer)objectToWrite);
			cellToWriteOn.setCellStyle(myStyles.get("rightStyle"));
		}
		else
		{
			cellToWriteOn.setCellValue(((String)objectToWrite).replaceAll("\"", "").replace("_"," "));
			cellToWriteOn.setCellStyle(myStyles.get("normalStyle"));
		}
		
	}
	
	
	
	/**
	 * Creates a cell boarder style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb){
		XSSFCellStyle style = (XSSFCellStyle)wb.createCellStyle();
		style.setBorderRight(BorderStyle.THIN);
		style.setRightBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderBottom(BorderStyle.THIN);
		style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderLeft(BorderStyle.THIN);
		style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderTop(BorderStyle.THIN);
		style.setTopBorderColor(IndexedColors.BLACK.getIndex());
		return style;
	}

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeStyles(XSSFWorkbook workbook)
	{
		myStyles = new Hashtable<>();
		XSSFCellStyle headerStyle = createBorderedStyle(workbook);
		Font boldFont = workbook.createFont();
		boldFont.setBold(true);
		boldFont.setColor(IndexedColors.WHITE.getIndex());
		boldFont.setFontHeightInPoints((short) 10);
		headerStyle.setFont(boldFont);
		headerStyle.setAlignment(HorizontalAlignment.CENTER);
		headerStyle.setVerticalAlignment(VerticalAlignment.TOP);
		headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(54, 96, 146)));
		headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		myStyles.put("headerStyle",headerStyle);

		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);

		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		headerStyle.setAlignment(HorizontalAlignment.CENTER);
		myStyles.put("normalStyle",normalStyle);
		
		XSSFCellStyle rightStyle = createBorderedStyle(workbook);
		rightStyle.setWrapText(true);
		rightStyle.setFont(normalFont);
		rightStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		rightStyle.setAlignment(HorizontalAlignment.RIGHT);
		myStyles.put("rightStyle",rightStyle);
		
		XSSFCellStyle leftStyle = createBorderedStyle(workbook);
		leftStyle.setWrapText(true);
		leftStyle.setFont(normalFont);
		leftStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		leftStyle.setAlignment(HorizontalAlignment.LEFT);
		myStyles.put("leftStyle",leftStyle);

		Font boldBodyFont = workbook.createFont();
		boldBodyFont.setFontHeightInPoints((short) 10);
		boldBodyFont.setBold(true);

		XSSFCellStyle boldStyle = createBorderedStyle(workbook);
		boldStyle.setWrapText(true);
		boldStyle.setFont(boldBodyFont);
		boldStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		myStyles.put("boldStyle",boldStyle);
		
		XSSFCellStyle accountStyle = createBorderedStyle(workbook);
		accountStyle.setFont(boldBodyFont);
		accountStyle.setVerticalAlignment(VerticalAlignment.CENTER);
		XSSFDataFormat df = wb.createDataFormat();
		accountStyle.setDataFormat(df.getFormat("$* #,##0.00"));
		myStyles.put("accountStyle", accountStyle);
	}
}	

	