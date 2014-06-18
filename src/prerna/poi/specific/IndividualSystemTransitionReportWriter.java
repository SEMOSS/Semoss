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
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 *This will take the information from the tasker generation processor and write a fact sheet report to an excel file for a given system name
 */
public class IndividualSystemTransitionReportWriter {

	Logger logger = Logger.getLogger(getClass());
	private XSSFWorkbook wb;
	private String systemName = "";
	private static String fileLoc = "";
	private static String templateLoc = "";
	
	private String beginIOCString = "June 10, 2016";
	private String iocString = "April 20, 2017";
	private String focString = "July 21, 2022";
	
	public IndividualSystemTransitionReportWriter(){
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
		templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\" + "Individual_System_Transition_Report_Template.xlsx";
	}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String systemName)
	{
		this.systemName = systemName;
		fileLoc += systemName+"_Transition_Report.xlsx";
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
	public boolean writeWorkbook()
	{
		boolean success = false;
		try{
			wb.setForceFormulaRecalculation(true);
			Utility.writeWorkbook(wb, fileLoc);
			success = true;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return success;
	}
	
	public static String getFileLoc(){
		return fileLoc;
	}
	
	/**
	 * Writes a generic list sheet from hashtable
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	public void writeListSheet(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		
		int indRowToWriteSystem = 3;
		XSSFRow rowToWriteSystem = sheetToWriteOver.getRow(indRowToWriteSystem);
		XSSFCell cellToWriteSystem = rowToWriteSystem.getCell(1);
		if(sheetName.contains("Interface"))
			cellToWriteSystem = rowToWriteSystem.getCell(3);
		String currString = cellToWriteSystem.getStringCellValue();
		currString = currString.replace("@SYSTEM@",systemName);
		cellToWriteSystem.setCellValue(currString);
		
		int rowToStart = 6;
		
		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn = sheetToWriteOver.createRow(rowToStart+row);

			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.createCell(col);
				if(resultRowValues[col] instanceof Double)
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				else if(resultRowValues[col] instanceof Integer)
					cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
				else
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
	
	public void writeHWSWSheet(String sheetName, HashMap<String,Object> resultBeforeIOC,HashMap<String,Object> resultIOC,HashMap<String,Object> resultFOC){
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),4,beginIOCString,8);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),33,iocString,45);
		writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),62,focString,74);
	}
	
	public void writeHWSWComponent(XSSFSheet sheetToWriteOver, ArrayList<Object[]> dataList,int rowToWriteData,String date, int rowToStartList){

		XSSFRow rowToWriteDate = sheetToWriteOver.getRow(rowToWriteData);
		XSSFCell cellToWriteDate = rowToWriteDate.getCell(2);
		String currString = cellToWriteDate.getStringCellValue();
		currString = currString.replace("@DATE@",date);
		cellToWriteDate.setCellValue(currString);
		
		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			
			XSSFRow rowToWriteOn = sheetToWriteOver.createRow(rowToStartList+row);
			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.createCell(col);
				if(resultRowValues[col] instanceof Double)
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				else if(resultRowValues[col] instanceof Integer)
					cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
				else
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
	
	public void writeSystemInfoSheet(String sheetName, HashMap<String,Object> result){
		
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		Object[] resultRowValues = dataList.get(0);		

		int indRowToWriteSystemName = 3;
		XSSFRow rowToWriteSystemName = sheetToWriteOver.getRow(indRowToWriteSystemName);
		XSSFCell cellToWriteSystemName = rowToWriteSystemName.createCell(0);
		cellToWriteSystemName.setCellValue((String)resultRowValues[0]);
		
		int indRowToWriteSystemDes = 6;
		XSSFRow rowToWriteSystemDes = sheetToWriteOver.getRow(indRowToWriteSystemDes);
		XSSFCell cellToWriteSystemDes = rowToWriteSystemDes.createCell(3);
		if(resultRowValues[1]!=null && ((String)resultRowValues[1]).length()>0)
			cellToWriteSystemDes.setCellValue((String)resultRowValues[1]);
		
		int rowToStartList = 9;
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStartList);

		for (int col=2; col< resultRowValues.length; col++) {
			XSSFCell cellToWriteOn = rowToWriteOn.createCell(col-2);
			if(resultRowValues[col] instanceof Double)
				cellToWriteOn.setCellValue((Double)resultRowValues[col]);
			else if(resultRowValues[col] instanceof Integer)
				cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
			else
				cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
		}
	}

}
