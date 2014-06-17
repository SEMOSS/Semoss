/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.poi.specific;

import java.io.File;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 *This will take the information from the tasker generation processor and write a fact sheet report to an excel file for a given system name
 */
public class IndividualSystemTransitionReportWriter {

	Logger logger = Logger.getLogger(getClass());
	XSSFWorkbook wb;
	String templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\" + "Individual_System_Transition_Report_Template.xlsx";
	String systemName;
	String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
	
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String systemName)
	{
		this.systemName = systemName;
		this.fileLoc += fileLoc + systemName+"_Transition_Report";
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
		
	}
	
	/**
	 * Writes the workbook to the file location.
	 */
	public void writeWorkbook()
	{
		wb.setForceFormulaRecalculation(true);
		Utility.writeWorkbook(wb, fileLoc);
	}
	
	/**
	 * Writes a generic list sheet from hashtable
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	public void writeListSheet(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		int rowToStart = 6;
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		
		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStart+row);

			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.getCell(col);
				if(resultRowValues[col] instanceof Double)
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				else
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
	
	public void writeHWSWSheet(String sheetName, HashMap<String,Object> resultBeforeIOC,HashMap<String,Object> resultIOC,HashMap<String,Object> resultFOC){
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),8);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),37);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),66);
	}
	
	public void writeHWSWComponent(XSSFSheet sheetToWriteOver, ArrayList<Object[]> dataList, int rowToStart){
		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStart+row);

			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.getCell(col);
				if(resultRowValues[col] instanceof Double)
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				else
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
	
	public void writeSystemInfoSheet(String sheetName, HashMap<String,Object> result){
		
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		int rowToStart = 9;
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		
		Object[] resultRowValues = dataList.get(0);
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStart);

		for (int col=0; col< resultRowValues.length; col++) {
			XSSFCell cellToWriteOn = rowToWriteOn.getCell(col);
			if(resultRowValues[col] instanceof Double)
				cellToWriteOn.setCellValue((Double)resultRowValues[col]);
			else
				cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
		}
	}

}
