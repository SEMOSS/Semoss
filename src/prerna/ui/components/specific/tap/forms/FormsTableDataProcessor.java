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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.ui.main.listener.specific.tap.FormsSourceFilesConsolidationListener;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FormsTableDataProcessor extends FormsDataProcessor {
	public static final Logger LOGGER = LogManager.getLogger(FormsTableDataProcessor.class.getName());	
	
	public String MY_SHEET_NAME = "";
	public int SYSTEM_NAME_COL_NUM = 0;
	
	
	public FormsTableDataProcessor(){
	}
	
	protected void updateData(XSSFWorkbook wb, 
			HashMap<String, HashMap<String, HashMap<String,String>>> map)throws IOException{
		
		XSSFSheet lSheet = getSheet(wb, MY_SHEET_NAME);
		
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		//LOGGER.info("********* lastRow: " + lastRow);
		
		for (String key : map.keySet()) {
			//LOGGER.info("********* key: " + key);
			LOGGER.info("********* Removing rows for .... : " + Utility.cleanLogString(key));
			removeRowsForSystem(lSheet, key, SYSTEM_NAME_COL_NUM);
			LOGGER.info("********* Done! Removing rows for .... : " + Utility.cleanLogString(key));
			
			LOGGER.info("********* Adding rows for .... : " + Utility.cleanLogString(key));
			addRowsForSystem(lSheet, key, map.get(key));
			LOGGER.info("********* Done! Adding rows for .... : " + Utility.cleanLogString(key));
		}
	}
	
	private void addRowsForSystem(XSSFSheet lSheet, String systemName, HashMap<String, HashMap<String, String>> valueMap1){
		int count = lSheet.getLastRowNum();
		for (String key1 : valueMap1.keySet()) {
			count++;
	    	//LOGGER.info("********* key1: " + key1);
	    	XSSFRow row = lSheet.createRow(count);
	    	XSSFCell cell0 = row.createCell(0);
			cell0.setCellValue(count-1);
			
			//setting interface name in col 5
	    	XSSFCell cell = row.createCell(SYSTEM_NAME_COL_NUM);
			cell.setCellValue(systemName);
			
			HashMap<String,String> valueMap2 = valueMap1.get(key1);
		    for (String key2 : valueMap2.keySet()) {
		    	//LOGGER.info("********* key2: " + key2);
		    	HashMap<String,String> valueMap3 = valueMap1.get(key1);
		    	for (String key3 : valueMap3.keySet()) {
		    		String value = valueMap3.get(key3);
		    		int n = getColumnNumber(key3);
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

	//Override this method in Sub-class
	public int getColumnNumber(String name){
		return -1;
	}
}
