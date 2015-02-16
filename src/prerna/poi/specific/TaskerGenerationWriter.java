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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.ConstantsTAP;
import prerna.util.Utility;

/**
 *This will take the information from the tasker generation processor and write a fact sheet report to an excel file for a given system name
 */
public class TaskerGenerationWriter {

	/**
	 * Retrieves the query results for a given system from the tasker generation processor and creates the tasker
	 * @param systemName		String containing the name of the system to create the tasker for
	 * @param fileLoc			String containing the file location to write the tasker to
	 * @param templateFileLoc	String containing the location of the tasker template
	 * @param systemDataHash	Hashtable containing all the query results from the tasker generation processor for a given system
	 */
	public void exportTasker(String systemName, String fileLoc, String templateFileLoc, Hashtable systemDataHash) {
		XSSFWorkbook wb;
		//if a report template exists, then create a copy of the template, otherwise create a new workbook
		if(templateFileLoc!=null) {
			try {
				wb = (XSSFWorkbook)WorkbookFactory.create(new File(templateFileLoc));
			} catch (InvalidFormatException e) {
				wb=new XSSFWorkbook();
			} catch (IOException e) {
				wb=new XSSFWorkbook();
			}
		} else {
			wb=new XSSFWorkbook();
		}

		//create an Arraylist of results for each query - retrieve results from Hashtable

		ArrayList systemNameResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_NAME_QUERY);
		ArrayList systemHighlightsResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY);
		ArrayList userTypesResults = (ArrayList) systemDataHash.get(ConstantsTAP.USER_TYPES_QUERY);
		ArrayList userInterfacesResults = (ArrayList) systemDataHash.get(ConstantsTAP.USER_INTERFACES_QUERY);

		ArrayList businessProcessResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUSINESS_PROCESS_QUERY);
		ArrayList activityResults = (ArrayList) systemDataHash.get(ConstantsTAP.ACTIVITY_QUERY);
		ArrayList bluResults = (ArrayList) systemDataHash.get(ConstantsTAP.BLU_QUERY);
		ArrayList dataResults = (ArrayList) systemDataHash.get(ConstantsTAP.DATA_QUERY);
		ArrayList icdResults = (ArrayList) systemDataHash.get(ConstantsTAP.LIST_OF_INTERFACES_QUERY);
//		ArrayList budgetResults = (ArrayList) systemDataHash.get(ConstantsTAP.BUDGET_QUERY);
		ArrayList siteResults = (ArrayList) systemDataHash.get(ConstantsTAP.SITE_LIST_QUERY);
		ArrayList ownerResults = (ArrayList) systemDataHash.get(ConstantsTAP.PPI_QUERY);
		ArrayList systemSWResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_SW_QUERY);
		ArrayList systemHWResults = (ArrayList) systemDataHash.get(ConstantsTAP.SYSTEM_HW_QUERY);
		ArrayList terrorResults = (ArrayList) systemDataHash.get(ConstantsTAP.TERROR_QUERY);	

		writeSystemOverviewSheet(wb, systemNameResults,systemHighlightsResults, userTypesResults, userInterfacesResults);
		writeMappingSheet(wb, "Business Processes",businessProcessResults);
		writeMappingSheet(wb, "Activities",activityResults);
		writeMappingSheet(wb, "Business Logic",bluResults);
		writeMappingSheet(wb, "Data Objects",dataResults);
		writeListOfInterfacesSheet(wb, ownerResults, icdResults);
//		writeBudgetSheet(wb, budgetResults);
		writeSiteListSheet(wb, siteResults);
		writeListSheet(wb, "Software",systemSWResults);
		writeListSheet(wb, "Hardware",systemHWResults);
		writeListSheet(wb, "Technical Gaps",terrorResults);

		wb.setForceFormulaRecalculation(true);
		Utility.writeWorkbook(wb, fileLoc);

	}

	/**
	 * Writes the System Overview Sheet in the workbook
	 * @param wb							XSSFWorkbook containing the System Overview Sheet to populate
	 * @param systemHighlightsResults		ArrayList containing the system highlights
	 * @param userTypesResults				ArrayList containing the type of users
	 * @param userInterfacesResults			ArrayList containing the type of user interfaces
	 */
	public void writeSystemOverviewSheet(XSSFWorkbook wb, ArrayList systemNameResults, ArrayList systemHighlightsResults, ArrayList userTypesResults, ArrayList userInterfacesResults) {
		XSSFSheet sheetToWriteOver = wb.getSheet("System Information");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(2);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(1);

		//write the systemname and acronym
		rowToWriteOn = sheetToWriteOver.getRow(2);
		for (int i=0; i<systemNameResults.size(); i++) {	
			ArrayList row = (ArrayList) systemNameResults.get(i);
			if (row.get(0) instanceof String) {
				String acronym= (String) row.get(0);
				cellToWriteOn=rowToWriteOn.getCell(3);
				cellToWriteOn.setCellValue(acronym);
			}
			if (row.get(1) instanceof String) {
				String fullName= (String) row.get(1);
				cellToWriteOn=rowToWriteOn.getCell(6);
				cellToWriteOn.setCellValue(fullName.replaceAll("\"","").replaceAll("_"," "));
			}
		}
		
		//System Highlights
		for (int i=0; i<systemHighlightsResults.size(); i++) {	
			ArrayList highlights = (ArrayList) systemHighlightsResults.get(i);			
			for (int j=0; j< highlights.size(); j++) {
				rowToWriteOn = sheetToWriteOver.getRow(6+j);
				cellToWriteOn = rowToWriteOn.getCell(3);
				if (highlights.get(j) instanceof String) {
					String highlight = (String) highlights.get(j);
					highlight = highlight.replaceAll("\"", "");					
					if ( (j==6 || j==7) && ( (highlight.length() >= 10) && (!highlight.equals("TBD")) && (!highlight.equals("")) && (!highlight.equals("NA")) ) ) {
						cellToWriteOn.setCellValue(highlight.substring(0, 10).replaceAll("_"," "));
					}
					else					
						cellToWriteOn.setCellValue(highlight.replaceAll("_"," "));
				}
				if (highlights.get(j) instanceof Double) {
					double highlight = (Double) highlights.get(j);
					cellToWriteOn.setCellValue(highlight);
				}
			}
		}

		//User Types
		int rowToWriteMax = 11; //max list of users
		for (int i=0; i<userTypesResults.size(); i++) {	
			ArrayList row = (ArrayList) userTypesResults.get(i);
			for (int j=0; j< row.size(); j++) {
				if (row.get(j) instanceof String) {
					String user= ((String) row.get(j)).replaceAll("_", " ");
					boolean inInitialList=false;
					for(int rowToWriteInd = 7;rowToWriteInd<rowToWriteMax;rowToWriteInd++)
					{
						rowToWriteOn = sheetToWriteOver.getRow(rowToWriteInd);
						cellToWriteOn=rowToWriteOn.getCell(5);
						if(cellToWriteOn.getStringCellValue().equalsIgnoreCase(user))
						{
							cellToWriteOn=rowToWriteOn.getCell(6);
							cellToWriteOn.setCellValue("X");
							inInitialList=true;
						}
					}
					if(!inInitialList)
					{
						rowToWriteOn = sheetToWriteOver.getRow(rowToWriteMax);
						cellToWriteOn=rowToWriteOn.getCell(5);
						cellToWriteOn.setCellValue(user);
						cellToWriteOn=rowToWriteOn.getCell(6);
						cellToWriteOn.setCellValue("X");
						rowToWriteMax++;
					}
				}
			}
		}

		//User Interfaces
		rowToWriteMax = 11; //max list of users
		for (int i=0; i<userInterfacesResults.size(); i++) {	
			ArrayList row = (ArrayList) userInterfacesResults.get(i);
			for (int j=0; j< row.size(); j++) {
				if (row.get(j) instanceof String) {
					String user= ((String) row.get(j)).replaceAll("_", " ");
					boolean inInitialList=false;
					for(int rowToWriteInd = 7;rowToWriteInd<rowToWriteMax;rowToWriteInd++)
					{
						rowToWriteOn = sheetToWriteOver.getRow(rowToWriteInd);
						cellToWriteOn=rowToWriteOn.getCell(8);
						if(cellToWriteOn.getStringCellValue().equalsIgnoreCase(user))
						{
							cellToWriteOn=rowToWriteOn.getCell(9);
							cellToWriteOn.setCellValue("X");
							inInitialList=true;
						}
					}
					if(!inInitialList)
					{
						rowToWriteOn = sheetToWriteOver.getRow(rowToWriteMax);
						cellToWriteOn=rowToWriteOn.getCell(8);
						cellToWriteOn.setCellValue(user);
						cellToWriteOn=rowToWriteOn.getCell(9);
						cellToWriteOn.setCellValue("X");
						rowToWriteMax++;
					}
				}
			}
		}

		
	}

	
	/**
	 * Writes the Mapping Sheets in the workbook
	 * Used for any sheet that has a list of instances and writes an X next to the ones that are mapped for this System
	 * @param wb					XSSFWorkbook containing the Mapping Sheet to populate
	 * @param mappingResults		ArrayList containing the mappings
	 */
	public void writeMappingSheet(XSSFWorkbook wb, String sheetName, ArrayList mappingResults) {
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);		
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(0);
		XSSFCellStyle style = cellToWriteOn.getCellStyle();
		int maxRow=sheetToWriteOver.getLastRowNum();
		
		XSSFCell cellToCheck;
		XSSFRow rowToCheck;
		ArrayList<String> defaultTaskerInfoList = new ArrayList<String>();
		//find all default tasker list items
		for(int rowCount=3;rowCount<maxRow+1;rowCount++)
		{
			rowToCheck = sheetToWriteOver.getRow(rowCount);
			cellToCheck = rowToCheck.getCell(0);
			String valToCheck = cellToCheck.getStringCellValue().replaceAll("_", " ").replaceAll("-"," ").replaceAll("/"," ");
			defaultTaskerInfoList.add(valToCheck);
		}		
		//go through each item on the sheet and find the match	
		for(int j = 0; j < defaultTaskerInfoList.size(); j++) {
			int rowCount = j + 3;
			rowToWriteOn = sheetToWriteOver.getRow(rowCount);
			for (int i=0; i<mappingResults.size(); i++) {	
				ArrayList<String> mappingResultsList = (ArrayList<String>) mappingResults.get(i);			
				String instance = ((String)mappingResultsList.get(1)).replaceAll("_", " ").replaceAll("-"," ").replaceAll("/"," ");
				if((defaultTaskerInfoList.get(j)).equals(instance))
				{
					cellToWriteOn=rowToWriteOn.getCell(1);
					if(mappingResultsList.size()>2)
						cellToWriteOn.setCellValue(((String)mappingResultsList.get(2)).replaceAll("\"", ""));
					else
						cellToWriteOn.setCellValue(1);
				}
				if ((!(defaultTaskerInfoList.contains(instance))) && j == 0)//Add any items not in the default list of information
				{
					rowToWriteOn = sheetToWriteOver.createRow(maxRow+1);
					
					cellToWriteOn=rowToWriteOn.createCell(0);
					cellToWriteOn.setCellStyle(style);
					cellToWriteOn.setCellValue(instance);
					
					cellToWriteOn=rowToWriteOn.createCell(1);
					cellToWriteOn.setCellStyle(style);
					if(mappingResultsList.size()>2) {
						cellToWriteOn.setCellValue(((String)mappingResultsList.get(2)).replaceAll("\"", "")); }
					else {
						cellToWriteOn.setCellValue(1);}					
					maxRow=sheetToWriteOver.getLastRowNum();
				}
			}
		}
	}
	
	
	/**
	 * Write the List of Interfaces Sheet in the workbook
	 * @param wb		XSSFWorkbook containing the List of Interfaces Sheet to populate
	 * @param result	ArrayList containing the interface query results
	 */
	public void writeListOfInterfacesSheet (XSSFWorkbook wb, ArrayList owner, ArrayList result) {
		String own = "TBD";
		if(owner.size()>0)
			own = (String) ((ArrayList)owner.get(0)).get(0);
		int count=1;
		while (count<owner.size())
		{
			if(((String) ((ArrayList)owner.get(count)).get(0)).equals("Central"))
				own = "Central";
			count++;
		}
		XSSFSheet sheetToWriteOver = wb.getSheet("ICDs");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(4);
		XSSFCell cellToWriteOn = rowToWriteOn.getCell(0);
		XSSFCellStyle style = cellToWriteOn.getCellStyle();
		if(result.size()==0)
			cellToWriteOn.setCellValue("TBD");
		else
		{
			if(result.size()>1)
				sheetToWriteOver.shiftRows(5, sheetToWriteOver.getLastRowNum(), result.size()-1);
			for (int i=0; i<result.size(); i++) {
				ArrayList resultRowValues = (ArrayList) result.get(i);
				rowToWriteOn = sheetToWriteOver.getRow(4+i);
				if(sheetToWriteOver.getRow(4+i)== null)
					rowToWriteOn = sheetToWriteOver.createRow(4+i);
				for (int j=0; j< resultRowValues.size()+1; j++) {
					cellToWriteOn = rowToWriteOn.getCell(j);
					if(cellToWriteOn==null)
					{
						cellToWriteOn=rowToWriteOn.createCell(j);
						cellToWriteOn.setCellStyle(style);
					}
					if(j==0)
						cellToWriteOn.setCellValue(own);
					else
						cellToWriteOn.setCellValue(((String)resultRowValues.get(j-1)).replaceAll("\"", "").replaceAll("_", " "));
				}
			}
		}
	}	

	/**
	 * Write the Budget Sheet in the workbook
	 * @param wb		XSSFWorkbook containing the Budget Sheet to populate
	 * @param budgetValues	ArrayList containing the budget query results
	 */
	public void writeBudgetSheet(XSSFWorkbook wb, ArrayList budgetValues) {
		XSSFSheet sheetToWriteOver = wb.getSheet("Budget");
		XSSFRow rowToWriteOn;
		XSSFCell cellToCheck;
		XSSFCell cellToWriteOn;
		int maxRow=sheetToWriteOver.getLastRowNum();
		for (int i=0; i<budgetValues.size(); i++) {	
			ArrayList budgetRowValues = (ArrayList) budgetValues.get(i);
			String instance = ((String)budgetRowValues.get(0)).replaceAll("_", " ").replaceAll("\"","");
			if(instance.equals("Applic- Mission"))
				instance = "Applic/ Mission";
			for(int rowCount=3;rowCount<maxRow;rowCount++)
			{
				rowToWriteOn = sheetToWriteOver.getRow(rowCount);
				if(rowToWriteOn!=null)
				{
				cellToCheck = rowToWriteOn.getCell(0);
				if(cellToCheck.getStringCellValue().equals(instance))
					for(int j=1;j<budgetRowValues.size();j++)
					{
						cellToWriteOn=rowToWriteOn.getCell(j);
						Object val = budgetRowValues.get(j);
						if(val instanceof Double)
						{
							if(!((Double)val).equals(0.0))
								cellToWriteOn.setCellValue((Double)val);
						}
						else
						{
							cellToWriteOn.setCellValue((String)val);
						}
					}
				}
			}
		}
	}
	/**
	 * Writes the data from the site list query to the Deployment Sheet
	 * @param wb		XSSFWorkbook containing the Deployment Sheet to populate
	 * @param result	ArrayList containing the output of the site list query
	 */
	public void writeSiteListSheet (XSSFWorkbook wb, ArrayList result){
		XSSFSheet sheetToWriteOver = wb.getSheet("Deployment");
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn=rowToWriteOn.getCell(0);
		XSSFCell formCellToWrite=rowToWriteOn.getCell(1);
		XSSFCellStyle style = cellToWriteOn.getCellStyle();
		XSSFCellStyle formstyle = formCellToWrite.getCellStyle();
		
			for (int i=0; i<result.size(); i++) {
				rowToWriteOn = sheetToWriteOver.getRow(3+i);
				if(rowToWriteOn==null)
					rowToWriteOn=sheetToWriteOver.createRow(3+i);
				cellToWriteOn = rowToWriteOn.getCell(0);
				if(cellToWriteOn==null)
				{
					cellToWriteOn=rowToWriteOn.createCell(0);
					cellToWriteOn.setCellStyle(style);
					for(int j=1;j<5;j++)
					{
						formCellToWrite=rowToWriteOn.createCell(j);
						formCellToWrite.setCellStyle(formstyle);
					}
				}
				ArrayList row = (ArrayList) result.get(i);
				cellToWriteOn.setCellValue((((String) row.get(0) ).replaceAll("\"", "")).replaceAll("_", " ") );
				for(int j=1;j<5;j++)
				{
					formCellToWrite = rowToWriteOn.getCell(j);
					formCellToWrite.setCellFormula("IFERROR(VLOOKUP(A"+(4+i)+",SiteDB2!$A$2:$E$3355,"+(j+1)+",FALSE),\"\")");
				}	
			}
	}
	
	
	/**
	 * Writes the data from the queries to the sheet specified in list format
	 * @param wb		XSSFWorkbook containing the sheet to populate
	 * @param sheetName	String containing the name of the sheet to populate
	 * @param result	ArrayList containing the output of the query
	 */
	public void writeListSheet(XSSFWorkbook wb, String sheetName, ArrayList result){
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		XSSFRow rowToWriteOn = sheetToWriteOver.getRow(3);
		XSSFCell cellToWriteOn=rowToWriteOn.getCell(0);
		XSSFCellStyle style = cellToWriteOn.getCellStyle();
		
		for (int i=0; i<result.size(); i++) {
			ArrayList resultRowValues = (ArrayList) result.get(i);
			rowToWriteOn = sheetToWriteOver.getRow(3+i);
			if(sheetToWriteOver.getRow(3+i)== null)
				rowToWriteOn = sheetToWriteOver.createRow(3+i);
			for (int j=0; j< resultRowValues.size(); j++) {
				cellToWriteOn = rowToWriteOn.getCell(j);
				if(cellToWriteOn==null)
				{
					cellToWriteOn=rowToWriteOn.createCell(j);
					cellToWriteOn.setCellStyle(style);
				}
				if(resultRowValues.get(j) instanceof Double)
					cellToWriteOn.setCellValue(((Double)resultRowValues.get(j)));
				else
					cellToWriteOn.setCellValue(((String)resultRowValues.get(j)).replaceAll("\"", "").replaceAll("_"," "));
			}
		}
	}
}
