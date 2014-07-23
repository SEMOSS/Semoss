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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import org.apache.poi.xssf.usermodel.XSSFCell;
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
	
	XSSFWorkbook wb = null;
	
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
		Date date = new Date();
		String writeFileName = "ConsolidatedSystemTransitionReport" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String fileLoc = workingDir + folder + writeFileName;
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
			ArrayList<Double> totalCostArray = new ArrayList<Double>();

			rowCount = writeHwSwRow(worksheet, rowCount, lpiSystem, totalCostArray);
			rowCount = writeInterfaceModernizationRow(worksheet, rowCount, lpiSystem, totalCostArray);
			rowCount = writeDiacapRow(worksheet, rowCount, lpiSystem, totalCostArray);
			
			rowCount = writeTotalCostsRow(worksheet, totalCostRow, lpiSystem, totalCostArray);
		}
	}
	
	private int writeTotalCostsRow(XSSFSheet worksheet, int rowCount, String systemName, ArrayList<Double> totalArray)
	{
		XSSFRow row = worksheet.createRow(0);
		setCell(row, 0, systemName);
		setCell(row, 1, ownerHashtable.get(systemName) + "");
		setCell(row, 2, "Total Expected Modernization Costs");

		Date atoDate = (Date) diacapHashtable.get(systemName);
		Calendar c = Calendar.getInstance();
		c.setTime(atoDate);
		int year = c.get(Calendar.YEAR);
		System.err.println("YEAR TESTING " + year);
		
		//// NEED TO ADD WRITING OF VALUES
		
		return rowCount++;
	}
	
	private int writeDiacapRow(XSSFSheet worksheet, int rowCount, String systemName, ArrayList<Double> totalArray)
	{
		XSSFRow row = worksheet.createRow(0);
		setCell(row, 0, systemName);
		setCell(row, 1, ownerHashtable.get(systemName) + "");
		setCell(row, 2, "System DIACAP");

		Date atoDate = (Date) diacapHashtable.get(systemName);
		Calendar c = Calendar.getInstance();
		c.setTime(atoDate);
		int year = c.get(Calendar.YEAR);
		System.err.println("YEAR TESTING " + year);
		
		return rowCount++;
	}
	
	private int writeHwSwRow(XSSFSheet worksheet, int rowCount, String systemName, ArrayList<Double> totalArray)
	{
		XSSFRow row = worksheet.createRow(0);
		setCell(row, 0, systemName);
		setCell(row, 1, ownerHashtable.get(systemName) + "");
		setCell(row, 2, "HW/SW Modernization");
		
		setCostCell(row, 3, (Double)hwswHashtable.get(systemName), totalArray);
		
		return rowCount++;
	}
	
	private int writeInterfaceModernizationRow(XSSFSheet worksheet, int rowCount, String systemName, ArrayList<Double> totalArray)
	{
		XSSFRow row = worksheet.createRow(0);
		setCell(row, 0, systemName);
		setCell(row, 1, ownerHashtable.get(systemName) + "");
		setCell(row, 2, "Interface Modernization**");
		
		setCostCell(row, 3, (Double)interfaceModHashtable.get(systemName), totalArray);
		
		return rowCount++;
	}
	
	private int writeBudgetRow(XSSFSheet worksheet, int rowCount, String systemName)
	{
		XSSFRow row = worksheet.createRow(0);
		setCell(row, 0, systemName);
		setCell(row, 1, ownerHashtable.get(systemName) + "");
		setCell(row, 2, "System Budget");
		
		Hashtable<String, Double> sysBudgetHash = this.budgetHashtable.get(systemName);
		setCell(row, 3, sysBudgetHash.get("FY15"));
		setCell(row, 4, sysBudgetHash.get("FY16"));
		setCell(row, 5, sysBudgetHash.get("FY17"));
		setCell(row, 6, sysBudgetHash.get("FY18"));
		setCell(row, 7, sysBudgetHash.get("FY19"));
		
		return rowCount++;
	}
	
	private void setCell(XSSFRow row, int index, String value)
	{
		XSSFCell cell = row.createCell(index);
		cell.setCellValue(value);
	}
	
	private void setCell(XSSFRow row, int index, Double value)
	{
		XSSFCell cell = row.createCell(index);
		cell.setCellValue(value);
	}
	
	private void setCostCell(XSSFRow row, int index, Double value, ArrayList<Double> total)
	{
		setCell(row, index, value);
		total.add(index, value);
	}

}
