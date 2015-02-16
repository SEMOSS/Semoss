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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;


public class ConsolidatedSystemReportWriter {

	ArrayList<String> lpiSystemList = new ArrayList<String>();
	ArrayList<String> lpniSystemList = new ArrayList<String>();
	Hashtable<String, Object> ownerHashtable = new Hashtable<String, Object>(); //systemName -> owner
	Hashtable<String, Hashtable<String, Double>> budgetHashtable = new Hashtable<String, Hashtable<String, Double>>(); //systemName -> year -> budget
	Hashtable<String, Object> hwswHashtable = new Hashtable<String, Object>(); //systemName -> cost
	Hashtable<String, Object> interfaceModHashtable = new Hashtable<String, Object>(); //systemName -> cost
	Hashtable<String, Object> diacapHashtable = new Hashtable<String, Object>(); //systemName -> diacapDate
	
	public ConsolidatedSystemReportWriter(ArrayList<String> lpiSystemList, ArrayList<String> lpniSystemList, Hashtable<String, Object> ownerHashtable, 
			Hashtable<String, Hashtable<String, Double>> budgetHashtable, Hashtable<String, Object> hwswHashtable, Hashtable<String, Object> interfaceModHashtable,
			Hashtable<String, Object> diacapHashtable)
	{
		this.lpiSystemList = lpiSystemList;
		this.lpniSystemList = lpniSystemList;
		this.ownerHashtable = ownerHashtable;
		this.budgetHashtable = budgetHashtable; 
		this.hwswHashtable = hwswHashtable;
		this.interfaceModHashtable = interfaceModHashtable;
		this.diacapHashtable = diacapHashtable;
	}
	
	public void runWriter() {
		XSSFWorkbook wb = new XSSFWorkbook();
		writeSheet(wb, "LPI", lpiSystemList);
		writeSheet(wb, "LPNI", lpniSystemList);

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = "\\export\\Reports\\";
		String writeFileName = "ConsolidatedSystemTransitionReport" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String fileLoc = workingDir + folder + writeFileName;
		
		formatExcel(wb);
		Utility.writeWorkbook(wb, fileLoc);
	}
	
	private void writeSheet(XSSFWorkbook wb, String tabName, ArrayList<String> systemList){
		XSSFSheet worksheet = wb.createSheet(tabName);
		
		//create header row
		XSSFRow headerRow = worksheet.createRow(0);
		setCell(headerRow, 0, "System Name");
		setCell(headerRow, 1, "System Owner");
		setCell(headerRow, 2, "Item");
		setCell(headerRow, 3, "FY15");
		setCell(headerRow, 4, "FY16");
		setCell(headerRow, 5, "FY17");
		setCell(headerRow, 6, "FY18");
		setCell(headerRow, 7, "FY19");
		
		int rowCount = 1;
		for(String lpiSystem: systemList) {
			rowCount = writeBudgetRow(worksheet, rowCount, lpiSystem);
			
			int totalCostRow = rowCount++; // save a row for total costs
			Double[] totalCostArray = new Double[5];

			rowCount = writeHwSwRow(worksheet, rowCount, lpiSystem, totalCostArray);
			rowCount = writeInterfaceModernizationRow(worksheet, rowCount, lpiSystem, totalCostArray);
			rowCount = writeDiacapRow(worksheet, rowCount, lpiSystem, totalCostArray);
			
			writeTotalCostsRow(worksheet, totalCostRow, lpiSystem, totalCostArray);
		}
	}
	
	private void formatExcel(XSSFWorkbook wb){
		
		XSSFCellStyle formatHeaders = formatExcelHeader(wb);
		XSSFCellStyle formatStrings = formatExcelStrings(wb);
		XSSFCellStyle formatExcelNumbers = formatExcelNumbers(wb);
		
		for(int sheetIdx = 0; sheetIdx < 2; sheetIdx++)
		{
			XSSFSheet worksheet = wb.getSheetAt(sheetIdx);
			for(int i = 0; i < 8; i++) {
				worksheet.getRow(0).getCell(i).setCellStyle(formatHeaders);
				for(int j = 1; j <= worksheet.getLastRowNum(); j++) {
					if(i < 3) {
						XSSFCell cell = worksheet.getRow(j).getCell(i);
						if(cell == null) {
							cell = worksheet.getRow(j).createCell(i);
						}
						cell.setCellStyle(formatStrings);
					} else {
						XSSFCell cell = worksheet.getRow(j).getCell(i);
						if(cell == null) {
							cell = worksheet.getRow(j).createCell(i);
						}
						cell.setCellStyle(formatExcelNumbers);
					}
				}
			}
			
			// autoformat cell column width after changing the format
			for(int i = 0; i < 8; i++) {
				worksheet.autoSizeColumn(i);
			}
		}
	}
	
	private void writeTotalCostsRow(XSSFSheet worksheet, int rowCount, String systemName, Double[] totalArray)
	{
		XSSFRow row = writeIntro(worksheet, rowCount, systemName, "Total Expected Modernization Costs");
		int cellCount = 3;
		for(Double val : totalArray){
			setCell(row, cellCount, val);
			cellCount++;
		}

		return;
	}
	
	private int writeDiacapRow(XSSFSheet worksheet, int rowCount, String systemName, Double[] totalArray)
	{
//		System.out.println("Diacap row " + rowCount);
		XSSFRow row = writeIntro(worksheet, rowCount, systemName, "System DIACAP");
		
		if(systemName.contains("ISITE")){
			System.out.println("ere");
		}

		String atoDateStr = (String) diacapHashtable.get(systemName);
		int year = 0;
		if(atoDateStr !=null && !atoDateStr.equals("NA")){
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		    Date atoDate;
			try {
				atoDate = df.parse(atoDateStr);
				Calendar c = Calendar.getInstance();
				c.setTime(atoDate);
				year = c.get(Calendar.YEAR);
//				System.err.println("YEAR TESTING " + year);
				if(year< 2012) {
					//requires immediate, same as missing date. taken care of later
				}
				else 
				{
					int cellNumForYear = year - 2012;
					if (year < 2014) // else two costs
					{
						setCostCell(row, cellNumForYear+3, 150000.0, totalArray);
						setCostCell(row, cellNumForYear+6, 150000.0, totalArray);
					}
					else if (year < 2017 ) // else just the one cost in the returned year
					{
						setCostCell(row, cellNumForYear+3, 150000.0, totalArray);
					}
					else {
						System.err.println("Large ATO Date " + year);
					}
				}
			} catch (ParseException e) {
				// ignored
			}
		}
		if (year < 2012) // cost in FY15 and FY18
		{
			setCostCell(row, 3, 150000.0, totalArray);
			setCostCell(row, 6, 150000.0, totalArray);
		}

		return rowCount + 1;
	}
	
	private int writeHwSwRow(XSSFSheet worksheet, int rowCount, String systemName, Double[] totalArray)
	{
//		System.out.println("hwsw row " + rowCount);
		XSSFRow row = writeIntro(worksheet, rowCount, systemName, "HW/SW Modernization");
		
		Object costObj = hwswHashtable.get(systemName);
		if(costObj instanceof Double){
			setCostCell(row, 3, (Double)costObj, totalArray);
		}

		return rowCount + 1;
	}
	
	private int writeInterfaceModernizationRow(XSSFSheet worksheet, int rowCount, String systemName, Double[] totalArray)
	{
//		System.out.println("mod row " + rowCount);
		XSSFRow row = writeIntro(worksheet, rowCount, systemName, "Interface Modernization**");

		Object costObj = interfaceModHashtable.get(systemName);
		if(costObj instanceof Double){
			setCostCell(row, 3, (Double)costObj, totalArray);
		}
		return rowCount + 1;
	}
	
	private int writeBudgetRow(XSSFSheet worksheet, int rowCount, String systemName)
	{
//		System.out.println("budget row " + rowCount);
		XSSFRow row = writeIntro(worksheet, rowCount, systemName, "System Budget");
		
		Hashtable<String, Double> sysBudgetHash = this.budgetHashtable.get(systemName);
		if (sysBudgetHash != null){
			setCell(row, 3, sysBudgetHash.get("FY15"));
			setCell(row, 4, sysBudgetHash.get("FY16"));
			setCell(row, 5, sysBudgetHash.get("FY17"));
			setCell(row, 6, sysBudgetHash.get("FY18"));
			setCell(row, 7, sysBudgetHash.get("FY19"));
		}

//		if(sysBudgetHash.get("FY15") != null) setCell(row, 3, sysBudgetHash.get("FY15"));
//		if(sysBudgetHash.get("FY16") != null) setCell(row, 4, sysBudgetHash.get("FY16"));
//		if(sysBudgetHash.get("FY17") != null) setCell(row, 5, sysBudgetHash.get("FY17"));
//		if(sysBudgetHash.get("FY18") != null) setCell(row, 6, sysBudgetHash.get("FY18"));
//		if(sysBudgetHash.get("FY19") != null) setCell(row, 7, sysBudgetHash.get("FY19"));
		
		return rowCount + 1;
	}
	
	private XSSFRow writeIntro(XSSFSheet sheet, int rowNumber, String systemName, String rowName){
		XSSFRow row = sheet.createRow(rowNumber);
		setCell(row, 0, Utility.getInstanceName(systemName));
		setCell(row, 1,(String) ownerHashtable.get(systemName));
		setCell(row, 2, rowName);
		return row;
	}
	
	private void setCell(XSSFRow row, int index, String value)
	{
		if(value != null){
			XSSFCell cell = row.createCell(index);
			cell.setCellValue(value);
		}
	}
	
	private void setCell(XSSFRow row, int index, Double value)
	{
		if(value != null){
			XSSFCell cell = row.createCell(index);
			cell.setCellValue(value);
		}
	}
	
	private void setCostCell(XSSFRow row, int index, Double value, Double[] total)
	{
		setCell(row, index, value);
		Double old = total[index - 3];
		if(old != null) {
			value = value + old;
		}
		total[index - 3] = value; // subtract three to account for the three columns of strings of every row
	}

	private XSSFCellStyle formatExcelHeader(XSSFWorkbook wb) 
	{
		XSSFCellStyle style = wb.createCellStyle();
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);

		XSSFFont font = wb.createFont();
		font.setBold(true);
		style.setFont(font);
		
		return style;
	}
	
	private XSSFCellStyle formatExcelStrings(XSSFWorkbook wb)
	{
		XSSFCellStyle style = wb.createCellStyle();
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		return style;
	}
	
	private XSSFCellStyle formatExcelNumbers(XSSFWorkbook wb) 
	{
		XSSFCellStyle style = wb.createCellStyle();
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		style.setDataFormat((short) 8);
		return style;
	}
	
}
