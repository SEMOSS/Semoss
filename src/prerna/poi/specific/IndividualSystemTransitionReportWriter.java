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
import org.apache.poi.hssf.record.CFRuleRecord.ComparisonOperator;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFConditionalFormattingRule;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFPatternFormatting;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFSheetConditionalFormatting;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 *This will take the information from the tasker generation processor and write a fact sheet report to an excel file for a given system name
 */
public class IndividualSystemTransitionReportWriter {

	static final Logger logger = LogManager.getLogger(IndividualSystemTransitionReportWriter.class.getName());
	private XSSFWorkbook wb;
	private String systemName = "";
	private static String fileLoc = "";
	private static String templateLoc = "";

	private String beginIOCString = "June 10, 2016";
	private String iocString = "April 20, 2017";
	private String focString = "July 21, 2022";

	public Hashtable<String,XSSFCellStyle> myStyles;

	public IndividualSystemTransitionReportWriter(){
	}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String systemName,String templateName)
	{
		this.systemName = systemName;
		
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
		templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\" + templateName;//"Individual_System_Transition_Report_Template.xlsx";

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

		makeStyles(wb);
	}

	/**
	 * Writes the workbook to the file location.
	 */
	public boolean writeWorkbook()
	{
		boolean success = false;
		try{
			wb.setForceFormulaRecalculation(true);
			wb.setSheetHidden(wb.getSheetIndex("Summary Charts"), true);
			Utility.writeWorkbook(wb, fileLoc);
			success = true;
		} catch (Exception ex) {
			success = false;
			ex.printStackTrace();
		}
		return success;
	}

	public static String getFileLoc(){
		return fileLoc;
	}
	
	public void writeBarChartData(String sheetName,int rowToStart,HashMap<String,Object> barHash) {
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		int[] dataList = (int[])barHash.get("data");
		
		for(int i=0;i<dataList.length;i++)
		{
			XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStart+i);
			XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
			cellToWriteOn.setCellValue(dataList[i]);
		}
	}

	public void writeSORSheet(String sheetName, HashMap<String,Object> result){
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		String[] headersList = (String[])result.get("headers");

		int rowToStart = 5;
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStart);
		XSSFCell cellToCopyFormat = rowToWriteOn.getCell(1);
		for(int col=2;col<headersList.length;col++)
		{
			XSSFCell cellToWriteOn = rowToWriteOn.createCell(col);
			cellToWriteOn.setCellStyle(cellToCopyFormat.getCellStyle());
			cellToWriteOn.setCellValue(((String)headersList[col]).replaceAll("\"", "").replaceAll("_"," "));
		}

		rowToStart++;
		rowToWriteOn = sheetToWriteOver.getRow(rowToStart);
		cellToCopyFormat = rowToWriteOn.getCell(1);
		for(int col=2;col<headersList.length;col++)
		{
			XSSFCell cellToWriteOn = rowToWriteOn.createCell(col);
			cellToWriteOn.setCellStyle(cellToCopyFormat.getCellStyle());
		}

		writeListSheet(sheetName,result);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		addConditionalFormatting(sheetName,result,rowToStart,rowToStart+dataList.size(),3,headersList.length);
	}

	public void addConditionalFormatting(String sheetName, HashMap<String,Object> result,int startRow, int lastRow, int firstCol, int lastCol)
	{
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
	    XSSFSheetConditionalFormatting sheetCF = sheetToWriteOver.getSheetConditionalFormatting();

	    XSSFConditionalFormattingRule rule1 = sheetCF.createConditionalFormattingRule(ComparisonOperator.GT, "0");
	    XSSFPatternFormatting patternFmt = rule1.createPatternFormatting();
	    patternFmt.setFillBackgroundColor(IndexedColors.GREEN.index);

//	    ConditionalFormattingRule rule2 = sheetCF.createConditionalFormattingRule(ComparisonOperator.BETWEEN, "-10", "10");
//	    XSSFConditionalFormattingRule [] cfRules ={rule1};

	    CellRangeAddress[] regions = new CellRangeAddress[]{new CellRangeAddress(startRow,lastRow,firstCol,lastCol)};
	    sheetCF.addConditionalFormatting(regions, rule1);

	}
	/**
	 * Writes a generic list sheet from hashtable
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param results	ArrayList containing the output of the query
	 */
	public void writeListSheet(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");

		fillStringInText(sheetToWriteOver,3,0,null,systemName);
		
		int rowToStart = 6;
		
		if(!sheetName.contains("Interface"))
			fillStringInText(sheetToWriteOver, 3,1,"@SYSTEM@",systemName);
		else 
			fillStringInText(sheetToWriteOver, 3,2,"@SYSTEM@",systemName);
//		else
//		{
//			fillStringInText(sheetToWriteOver, 3,3,"@SYSTEM@",systemName);
//			rowToStart = 9;
//		}


		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn;
			XSSFRow rowToCopyFormat = sheetToWriteOver.getRow(rowToStart);
			if(row==0)
			{
				rowToWriteOn = sheetToWriteOver.getRow(rowToStart+row);
			}
			else
			{
				rowToWriteOn = sheetToWriteOver.createRow(rowToStart+row);
				rowToWriteOn.setRowStyle(rowToCopyFormat.getRowStyle());
			}
			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn;
				if(row==0)
					cellToWriteOn = rowToWriteOn.getCell(col);
				else
				{
					cellToWriteOn = rowToWriteOn.createCell(col);
					XSSFCell cellToCopyFormat = rowToCopyFormat.getCell(col);
					cellToWriteOn.setCellStyle(cellToCopyFormat.getCellStyle());
				}
				if(resultRowValues[col] instanceof Double) {
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				}
				else if(resultRowValues[col] instanceof Integer) {
					cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
				}
				else {
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
				}
			}
		}

		// for System Interfaces Sheet
		if(result.get("directCost") != null && result.get("indirectCost") != null)
		{
			double totalDirectCost = (Double) result.get("directCost");
			double totalIndirectCost = (Double) result.get("indirectCost");
			int lastRowNum = sheetToWriteOver.getLastRowNum();
			XSSFRow lastRow = sheetToWriteOver.getRow(lastRowNum);
			int lastCellNum = lastRow.getLastCellNum();

			if(dataList.size()>0)
			{				
				lastRow = sheetToWriteOver.createRow(lastRowNum+1);
				XSSFCell cellToWriteOn = lastRow.createCell(lastCellNum-2);
				cellToWriteOn.setCellStyle(myStyles.get("accountStyle"));
				cellToWriteOn.setCellValue(totalDirectCost);
				cellToWriteOn = lastRow.createCell(lastCellNum-1);
				cellToWriteOn.setCellStyle(myStyles.get("accountStyle"));
				cellToWriteOn.setCellValue(totalIndirectCost);
			}
		}
	}

	public void writeHWSWSheet(String sheetName, HashMap<String,Object> resultBeforeIOC,HashMap<String,Object> resultIOC,HashMap<String,Object> resultFOC){
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		
		fillStringInText(sheetToWriteOver,2,0,null,systemName);
		
		int numRows = ((ArrayList<Object[]>)resultBeforeIOC.get("data")).size();
		int numCols = 7;
		XSSFRow rowToCopyFormat = sheetToWriteOver.getRow(9);

		for(int i=1;i<numRows;i++)
		{
			XSSFRow newRow = sheetToWriteOver.createRow(9+i);
			newRow.setRowStyle(rowToCopyFormat.getRowStyle());
			for(int j=0;j<numCols;j++)
			{
				XSSFCell cellToCopyFormat = rowToCopyFormat.getCell(j);
				XSSFCell newCell = newRow.createCell(j);
				newCell.setCellStyle(cellToCopyFormat.getCellStyle());
			}
		}
		XSSFRow newRow = sheetToWriteOver.createRow(9+numRows);

		if(numRows==0)
		{
			createFormatedHWSWCells(sheetToWriteOver,"IOC",6,4+numRows,11+numRows, numCols);
			createFormatedHWSWCells(sheetToWriteOver,"FOC",6,4+numRows,6+2*(5+numRows), numCols);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),6,beginIOCString,9);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultIOC.get("data"),11+numRows,iocString,14+numRows);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultFOC.get("data"),16+2*(numRows),focString,19+2*(numRows));
		}
		else
		{
			createFormatedHWSWCells(sheetToWriteOver,"IOC",6,4+numRows,11+numRows-1, numCols);
			createFormatedHWSWCells(sheetToWriteOver,"FOC",6,4+numRows,6+2*(5+numRows-1), numCols);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultBeforeIOC.get("data"),6,beginIOCString,9);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultIOC.get("data"),11+numRows-1,iocString,14+numRows-1);
			writeHWSWComponent(sheetToWriteOver,(ArrayList<Object[]>)resultFOC.get("data"),16+2*(numRows-1),focString,19+2*(numRows-1));
		}
	}
	public void createFormatedHWSWCells(XSSFSheet sheetToWriteOver,String timeline,int rowToStartCopy, int numRowsToCopy, int rowToStartWrite, int numCols)
	{
		for(int i=0;i<numRowsToCopy;i++)
		{
			XSSFRow rowToCopy = sheetToWriteOver.getRow(rowToStartCopy+i);
			if(rowToCopy!=null)
			{
				XSSFRow newRow = sheetToWriteOver.createRow(rowToStartWrite+i);
				newRow.setRowStyle(rowToCopy.getRowStyle());
				for(int j=0;j<numCols;j++)
				{
					XSSFCell cellToCopy = rowToCopy.getCell(j);
					if(cellToCopy!=null)
					{
						XSSFCell newCell = newRow.createCell(j);
						newCell.setCellStyle(cellToCopy.getCellStyle());
						if(i!=0)
							newCell.setCellValue(cellToCopy.getStringCellValue());
						else
						{
							if(j==0)
								newCell.setCellValue(timeline);
							if(j==2)
								newCell.setCellValue(cellToCopy.getStringCellValue().replace("the start of IOC",timeline));
						}
					}
				}
			}
		}
		sheetToWriteOver.addMergedRegion(new CellRangeAddress(rowToStartWrite, rowToStartWrite, 0, 1));
		sheetToWriteOver.addMergedRegion(new CellRangeAddress(rowToStartWrite, rowToStartWrite, 2, 6));

	}
	public void writeHWSWComponent(XSSFSheet sheetToWriteOver, ArrayList<Object[]> dataList,int rowToWriteData,String date, int rowToStartList){

		fillStringInText(sheetToWriteOver,rowToWriteData,2,"@DATE@",date);

		for (int row=0; row<dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			XSSFRow rowToWriteOn;
			rowToWriteOn = sheetToWriteOver.getRow(rowToStartList+row);
			for (int col=0; col< resultRowValues.length; col++) {
				XSSFCell cellToWriteOn;
				cellToWriteOn = rowToWriteOn.getCell(col);
				if(resultRowValues[col] instanceof Double)
					cellToWriteOn.setCellValue((Double)resultRowValues[col]);
				else if(resultRowValues[col] instanceof Integer)
					cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
				else if(col==1)
				{
					String dateString = (String)resultRowValues[col];
					dateString = dateString.substring(0,dateString.indexOf("T"));
					cellToWriteOn.setCellValue(dateString.replaceAll("\"", "").replaceAll("_"," "));
				}
				else
					cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
	public void writeModernizationTimelineSheet(String sheetName, HashMap<String,Object> software, HashMap<String,Object> hardware, HashMap<String,Object> budget)
	{
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);

		int indRowToWriteSystem = 3;

		fillStringInText(sheetToWriteOver,indRowToWriteSystem,0,null,systemName);
		fillStringInText(sheetToWriteOver, indRowToWriteSystem,1,"@SYSTEM@",systemName);

		writeModernizationTimeline(sheetName,6,(ArrayList<Object[]>)software.get("data"));
		writeModernizationTimeline(sheetName,7,(ArrayList<Object[]>)hardware.get("data"));

		ArrayList<Object[]> budgetList = (ArrayList<Object[]>)budget.get("data");
		if(budgetList!=null)
		{
			int indRowToWriteBudget = 9;
			for(int i=0;i<budgetList.size();i++)
			{
				Object[] budgetRow = budgetList.get(i);
				XSSFRow rowToWriteOn;
				if(((String)budgetRow[1]).contains("SW"))
					rowToWriteOn = sheetToWriteOver.getRow(indRowToWriteBudget);
				else
					rowToWriteOn = sheetToWriteOver.getRow(indRowToWriteBudget+1);
				XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);
				if(budgetRow[2] instanceof Double)
					cellToWriteOn.setCellValue((Double)budgetRow[2]);
				else if(budgetRow[2] instanceof Integer)
					cellToWriteOn.setCellValue((Integer)budgetRow[2]);
				else
					cellToWriteOn.setCellValue((String)budgetRow[2]);
		
			}
		}

	}
	public void writeModernizationTimeline(String sheetName,int rowToWriteList,ArrayList<Object[]> dataList)
	{
		ArrayList<Integer> yearList = new ArrayList<Integer>();
		ArrayList<Double> budgetList = new ArrayList<Double>();
		for(int year=2015;year<=2022;year++)
		{
			yearList.add(year);
			budgetList.add(0.0);
		}
		for(int i=0;i<dataList.size();i++)
		{
			Object[] swHWRow = dataList.get(i);
			String date = (String)swHWRow[1];
			if(date!=null&&date.length()>0)
			{
				double cost = (Double)swHWRow[5];
				int indexOfHyphen = date.indexOf("-");
				if(indexOfHyphen>0)
				{
					int year = Integer.parseInt(date.substring(0,indexOfHyphen));
					int yearIndex = yearList.indexOf(year);
					if(year<yearList.get(0))
						yearIndex = 0;
					if(yearIndex>-1)
						budgetList.set(yearIndex, budgetList.get(yearIndex)+cost);
				}
			}
		}

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);

		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToWriteList);

		double totalCost = 0.0;
		for (int budgetInd=0; budgetInd<budgetList.size(); budgetInd++) {
			XSSFCell cellToWriteOn = rowToWriteOn.getCell(budgetInd+1);
			cellToWriteOn.setCellValue(budgetList.get(budgetInd));
			totalCost+=budgetList.get(budgetInd);
		}
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(budgetList.size()+1);
		cellToWriteOn.setCellValue(totalCost);


	}
	

	public void writeSystemInfoSheet(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		Object[] resultRowValues = dataList.get(0);		

		fillStringInText(sheetToWriteOver,3,1,null,systemName);

		String sysDescription = "";
		if(resultRowValues[1]!=null && ((String)resultRowValues[1]).length()>0)
			sysDescription =((String)resultRowValues[0]).replaceAll("\"", "").replaceAll("_"," ");
		fillStringInText(sheetToWriteOver,6,3,null,sysDescription);

		int rowToStartList = 10;
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStartList);

		for (int col=0; col< resultRowValues.length-1; col++) {
			XSSFCell cellToWriteOn = rowToWriteOn.getCell(col*2+1);
			//cellToWriteOn.setCellStyle((XSSFCellStyle)myStyles.get("normalStyle"));
			if(resultRowValues[col+1] instanceof Double)
				cellToWriteOn.setCellValue((Double)resultRowValues[col+1]);
			else if(resultRowValues[col+1] instanceof Integer)
				cellToWriteOn.setCellValue((Integer)resultRowValues[col+1]);
			else if(resultRowValues[col+1] instanceof String) {
				String stringToWrite = ((String)resultRowValues[col+1]).replaceAll("\"", "").replaceAll("_"," ");
				if(stringToWrite.length()==0||stringToWrite.equals("NA"))
					stringToWrite = "TBD";
				cellToWriteOn.setCellValue(stringToWrite);
			}
			else
				logger.info("Writing System Overview Sheet but improper type for cell at row "+rowToStartList+" col "+(col+1));
		}
		fillStringInText(sheetToWriteOver, 13, 1,"@SYSTEM@",systemName);
		fillStringInText(sheetToWriteOver, 33, 1,"@SYSTEM@",systemName);
		fillStringInText(sheetToWriteOver,13,1,"@DATE@",beginIOCString);
		
	}
	
	public void writeSystemSiteDetails(String sheetName, HashMap<String,Object> result){

		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>)result.get("data");
		if(dataList==null || dataList.size()==0)
			return;
		Object[] resultRowValues = dataList.get(0);

		int rowToStartList = 53;
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(rowToStartList);

		for (int col=0; col< resultRowValues.length; col++) {
			XSSFCell cellToWriteOn = rowToWriteOn.getCell(col*4+1);
			if(resultRowValues[col] instanceof Double)
				cellToWriteOn.setCellValue((Double)resultRowValues[col]);
			else if(resultRowValues[col] instanceof Integer)
				cellToWriteOn.setCellValue((Integer)resultRowValues[col]);
			else
				cellToWriteOn.setCellValue(((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," "));
		}		
	}
	
	public void fillStringInText(XSSFSheet sheetToWriteOver, int row, int col,String textToFind,String textToReplace){
		XSSFRow rowToSetSystemName = sheetToWriteOver.getRow(row);
		XSSFCell cellToSetSystemName = rowToSetSystemName.getCell(col);
		if(textToFind == null)
			cellToSetSystemName.setCellValue(textToReplace);
		else{
			String currString = cellToSetSystemName.getStringCellValue();
			currString = currString.replaceAll(textToFind,textToReplace);
			cellToSetSystemName.setCellValue(currString);
		}
	}

	/**
	 * Creates a cell boarder style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb){
		XSSFCellStyle style = (XSSFCellStyle)wb.createCellStyle();
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderTop(CellStyle.BORDER_THIN);
		style.setTopBorderColor(IndexedColors.BLACK.getIndex());
		return style;
	}

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeStyles(XSSFWorkbook workbook)
	{
		myStyles = new Hashtable<String,XSSFCellStyle>();
		XSSFCellStyle headerStyle = createBorderedStyle(workbook);
		Font boldFont = workbook.createFont();
		boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		boldFont.setColor(IndexedColors.WHITE.getIndex());
		boldFont.setFontHeightInPoints((short) 10);
		headerStyle.setFont(boldFont);
		headerStyle.setAlignment(CellStyle.ALIGN_CENTER);
		headerStyle.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(54, 96, 146)));
		headerStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
		myStyles.put("headerStyle",headerStyle);

		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);

		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("normalStyle",normalStyle);

		Font boldBodyFont = workbook.createFont();
		boldBodyFont.setFontHeightInPoints((short) 10);
		boldBodyFont.setBoldweight(Font.BOLDWEIGHT_BOLD);

		XSSFCellStyle boldStyle = createBorderedStyle(workbook);
		boldStyle.setWrapText(true);
		boldStyle.setFont(boldBodyFont);
		boldStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("boldStyle",boldStyle);
		
		XSSFCellStyle accountStyle = createBorderedStyle(workbook);
		accountStyle.setFont(boldBodyFont);
		accountStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		XSSFDataFormat df = wb.createDataFormat();
		accountStyle.setDataFormat(df.getFormat("$* #,##0.00"));
		myStyles.put("accountStyle", accountStyle);
		

	}
}
