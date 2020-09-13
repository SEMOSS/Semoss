/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)


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
package prerna.ui.components.specific.tap.forms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FormsICDDataProcessor extends BaseFormsICDDataProcessor {
	public static final Logger LOGGER = LogManager.getLogger(FormsICDDataProcessor.class.getName());	
	
	/*public String INTERFACES_FILE = "\\ICD Consolidated to Service Mapping_Validated.xlsm";
	
	public String MY_QUERY = SYSTEM_INTERFACE_ENTERPRISE_QUERY;
	
	//Constants for ICD Tab;
	public int INTERFACE_NAME_COL_NUM = 5;
	public int PROTOCOL_COL_NUM = 8;
	public int FORMAT_COL_NUM = 10;
	public int FREQUENCY_COL_NUM = 9;
	public int PROVIDER_COL_NUM = 2;
	public int CONSUMER_COL_NUM = 3;
	public int ICD_DATA_OBJECT_STARTING_COL_NUM = 60;
	public int SHEET_LAST_COLUMN = 60;
	
	public int ICD_FREQUENCY_STARTING_COL_NUM = 0;
	public int ICD_FREQUENCY_ENDING_COL_NUM = 0;
	public int ICD_PROTOCOL_STARTING_COL_NUM = 0;
	public int ICD_PROTOCOL_ENDING_COL_NUM = 0;
	public int ICD_FORMAT_STARTING_COL_NUM = 0;
	public int ICD_FORMAT_ENDING_COL_NUM = 0;
	
	public static final String SYSTEM_INTERFACES_SHEET_NAME = "ICD";
		
	public static HashMap<String, Integer> ICD_DATA_OBJECT_HEADER_CACHE = null;
	public static HashMap<String, Integer> ICD_FREQUENCY_HEADER_CACHE = null;
	public static HashMap<String, Integer> ICD_PROTOCOL_HEADER_CACHE = null;
	public static HashMap<String, Integer> ICD_FORMAT_HEADER_CACHE = null;
	*/
	public FormsICDDataProcessor(){
		//MY_QUERY = SYSTEM_INTERFACE_ENTERPRISE_QUERY;
	}
	/*
	public void processICDFile(IEngine engine, File sourceFolder, ArrayList<String> systemsList){
		try{
			LOGGER.info("********** Querying for System Interfaces");
			HashMap<String, HashMap<String, HashMap<String,String>>> map = 
					getDataForTables(MY_QUERY, systemsList, engine);
			//LOGGER.info("********** interfaces size: " + map.size());
			
			//tableToString(map);
			String fileName = sourceFolder.getAbsolutePath() + INTERFACES_FILE;
			XSSFWorkbook wb = getWorkBook(fileName);
			
			updateSystemInterfaces(wb, map);
			
			Utility.writeWorkbook(wb, fileName);
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
	}
	
	protected void updateSystemInterfaces(XSSFWorkbook wb, 
			HashMap<String, HashMap<String, HashMap<String,String>>> map)throws IOException{

		XSSFSheet lSheet = getSheet(wb, SYSTEM_INTERFACES_SHEET_NAME);
		//lSheet = cleanupSheet(lSheet);
		ICD_DATA_OBJECT_HEADER_CACHE = updateHeaderCache(wb, lSheet, ICD_DATA_OBJECT_HEADER_CACHE, ICD_DATA_OBJECT_STARTING_COL_NUM);
		LOGGER.info("********* Laoding ICD_FREQUENCY_HEADER_CACHE");
		ICD_FREQUENCY_HEADER_CACHE = updateHeaderCache(wb, lSheet, ICD_FREQUENCY_HEADER_CACHE, 
																ICD_FREQUENCY_STARTING_COL_NUM, ICD_FREQUENCY_ENDING_COL_NUM);
		LOGGER.info("********* Done Laoding ICD_FREQUENCY_HEADER_CACHE");
		ICD_PROTOCOL_HEADER_CACHE = updateHeaderCache(wb, lSheet, ICD_PROTOCOL_HEADER_CACHE, 
																	ICD_PROTOCOL_STARTING_COL_NUM, ICD_PROTOCOL_ENDING_COL_NUM);
		ICD_FORMAT_HEADER_CACHE = updateHeaderCache(wb, lSheet, ICD_FORMAT_HEADER_CACHE, 
																			ICD_FORMAT_STARTING_COL_NUM, ICD_FORMAT_ENDING_COL_NUM);
		
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		//LOGGER.info("********* lastRow: " + lastRow);
		
		for (String key : map.keySet()) {
			//LOGGER.info("********* key: " + key);
			int count = 0;
			LOGGER.info("********* Removing interfaces for .... : " + key);
			for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
		    	count = rIndex;
		    	//LOGGER.info("********* rIndex: " + rIndex);
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure row is not null
				if (row != null) {
					XSSFCell systemCell1 = row.getCell(PROVIDER_COL_NUM);
					XSSFCell systemCell2 = row.getCell(CONSUMER_COL_NUM);
					if (systemCell1 != null && systemCell2 != null ) {
						// get the name of the System from the sheet
						String system1Name = systemCell1.getStringCellValue();
						String system2Name = systemCell2.getStringCellValue();
						// Find the row you are looking for
						if (system1Name.equals(key) || system2Name.equals(key)) {
							//LOGGER.info("********* system1Name: " + system1Name + " system2Name: " + system2Name + "********* Removing row");
							lSheet.removeRow(row);
							//LOGGER.info("********* Removed");
							//lSheet.shiftRows(rIndex+1, lastRow, -1);
							//rIndex = rIndex-1;
						}
					}
				}
			}
			LOGGER.info("********* Removed interfaces for .... : " + key);
			
		    HashMap<String, HashMap<String,String>> valueMap1 = map.get(key);
		    for (String key1 : valueMap1.keySet()) {
		    	//LOGGER.info("********* key1: " + key1);
		    	XSSFRow row = lSheet.createRow(count++);
		    	XSSFCell cell0 = row.createCell(0);
				cell0.setCellValue(count-1);
				
				//setting interface name in col 5
		    	//XSSFCell cell = row.createCell(INTERFACE_NAME_COL_NUM);
				//cell.setCellValue(key1);
				addInterfaceName(row, key1);
				
				HashMap<String,String> valueMap2 = valueMap1.get(key1);
			    for (String key2 : valueMap2.keySet()) {
			    	//LOGGER.info("********* key2: " + key2);
			    	HashMap<String,String> valueMap3 = valueMap1.get(key1);
			    	for (String key3 : valueMap3.keySet()) {
			    		String value = valueMap3.get(key3);
			    		if(key3.equalsIgnoreCase("DataObject")){
			    			int n = getDataObjectColNum(value);
				    		if(n!= -1){
				    			XSSFCell cell1 = row.createCell(n);
				    			//LOGGER.info("********* valueMap3.get(key3): " +value);
					    		cell1.setCellValue("X");
				    		}
			    		}
			    		else {
				    		//String value = valueMap3.get(key3);
				    		if(key3.equalsIgnoreCase("Frequency")){
				    			tagValue(row, value, ICD_FREQUENCY_HEADER_CACHE);
				    		}
				    		else if(key3.equalsIgnoreCase("Protocol")){
				    			tagValue(row, value, ICD_PROTOCOL_HEADER_CACHE);
				    		}
				    		else if(key3.equalsIgnoreCase("Format")){
				    			tagValue(row, value, ICD_FORMAT_HEADER_CACHE);
				    		}
				    		int n = getICDColumnNumber(key3);
				    		//LOGGER.info("********* key3: " + key3 + " Col Num: " + n);
				    		if(n!= -1){
				    			XSSFCell cell1 = row.createCell(n);
				    			//LOGGER.info("********* valueMap3.get(key3): " +value);
					    		cell1.setCellValue(value);
				    		}
			    		}
			    	}
			    }
		    }
		}
	}
	
	public void tagValue(XSSFRow row, String value, HashMap<String, Integer> headerCache){
		int n = getColNumFromCache(value, headerCache);
		if(n!= -1){
			XSSFCell cell1 = row.createCell(n);
			cell1.setCellValue("S");
		}
	}
	
	public void addInterfaceName(XSSFRow row, String key1){
		XSSFCell cell = row.createCell(INTERFACE_NAME_COL_NUM);
		cell.setCellValue(key1);
	}
	
	public int getICDColumnNumber(String name){
		switch (name)
	    {
	      case "Format":
	    	  return FORMAT_COL_NUM;
	      case "Consumer":
	    	  return CONSUMER_COL_NUM;
	      case "Frequency":
	    	  return FREQUENCY_COL_NUM;
	      case "Protocol":
	    	  return PROTOCOL_COL_NUM;
	      case "Provider":
	    	  return PROVIDER_COL_NUM;
	      default:
	          return -1;
	    }
	}
	
	private int getDataObjectColNum(String dataObjectName){
		String cleanedString = trimSpecialCharacters(dataObjectName);
		if(ICD_DATA_OBJECT_HEADER_CACHE.get(cleanedString)!=null) {
	    	int n = ICD_DATA_OBJECT_HEADER_CACHE.get(cleanedString);
	    	return n;
	    } 
    	else {
    		LOGGER.info("Could not find " + cleanedString + " in sheet");
    	}
		return -1;
	}
	*/
/*
	private XSSFSheet cleanupSheet(XSSFSheet lSheet){
		int lastRowNum = lSheet.getLastRowNum();
		XSSFRow headerRow = lSheet.getRow(0);
		LOGGER.info("********* lastRow: " + lastRowNum);
		int count = 0;
	    for (int rIndex = 1; rIndex <= lastRowNum; rIndex++) {
	    	count = rIndex;
	    	//LOGGER.info("********* rIndex: " + rIndex);
			XSSFRow row = lSheet.getRow(rIndex);
			// check to make sure row is not null
			if (row != null) {
				XSSFCell col1 = row.getCell(0);
				//if (col1 == null || row.getCell(0).getStringCellValue()== null || 
				//		row.getCell(0).getStringCellValue().equals("")) {
						//lSheet.copyRows(srcStartRow, srcEndRow, destStartRow, cellCopyPolicy);
					XSSFRow lastRow = lSheet.getRow(lastRowNum);
					copyRow(headerRow, lastRow, row, rIndex);
					lSheet.removeRow(lastRow);
					lastRowNum = lastRowNum - 1;
			}
		}
		return lSheet;
	}
	
	private void copyRow(XSSFRow headerRow, XSSFRow sourceRow, XSSFRow destinationRow, int index){
		LOGGER.info("********* copying index: " + index);
		XSSFCell c1 = destinationRow.createCell(0);
		c1.setCellValue(index-1);
		//TODO - complete this method
		int colCount = 1;
		while (headerRow.getCell(colCount)!=null && 
			headerRow.getCell(colCount).getStringCellValue()!=null && 
			!headerRow.getCell(colCount).getStringCellValue().equals("")){
			
			XSSFCell c = destinationRow.createCell(colCount);
			if(sourceRow.getCell(colCount) != null){
				c.setCellValue(sourceRow.getCell(colCount).getStringCellValue());
			}
			colCount++;
		}
	}
	*/
}
