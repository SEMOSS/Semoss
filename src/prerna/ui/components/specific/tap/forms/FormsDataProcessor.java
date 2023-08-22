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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import cern.colt.Arrays;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class FormsDataProcessor extends BaseFormsDataProcessor{
	public static final Logger LOGGER = LogManager.getLogger(FormsDataProcessor.class.getName());

	//List of files
	public static final String TASKER_FILE = DIR_SEPARATOR + "Tasker Responses Collection Worksheet.xlsx";
	public static final String BLU_FILE = DIR_SEPARATOR + "System BLU.xlsx";
	public static final String SITE_FILE = DIR_SEPARATOR + "TAP_Site_Data-Loader.xlsm";
	public static final String ICD_FILE = DIR_SEPARATOR + "ICD Consolidated to Service Mapping_Validated.xlsm";
	public static final String SITE_ICD_FILE = DIR_SEPARATOR + "ICD SiteSpecific.xlsm";
	public static final String SYS_DATA_FILE = DIR_SEPARATOR + "System Data.xlsx";
	
	public static final String SYSTEM_INFORMATION_SHEET_NAME = "System_Information";
	public static final String PERSONNEL_SHEET_NAME = "System_to_Personnel";
	public static final String USER_INTERFACE_SHEET_NAME = "System-UI";
	public static final String ACTIVITY_SHEET_NAME = "System_to_Activity";
	public static final String BUSINESS_PROCESS_SHEET_NAME = "System_to_BP";
	public static final String BLU_SHEET_NAME = "System - BLU";
	public static final String DCSITE_SHEET_NAME = "System-To-Deployment";
	public static final String MTF_SHEET_NAME = "System-To-Availability";
	public static final String DATA_OBJECT_SHEET_NAME = "System-Data";		

	public static int AVAILABILITY_ACTUAL_COL_NUM = 18;
	public static int AVAILABILITY_REQUIRED_COL_NUM = 17;
	public static int DESCRIPTION_COL_NUM = 3;
	public static int END_OF_SUPPORT_DATE_COL_NUM = 22;
	public static int USER_CONSOLES_COL_NUM = 16;
	public static int FULL_SYSTEM_NAME_COL_NUM = 2;
	public static int NUM_OF_USERS_COL_NUM = 15;
	public static int TRANSACTION_COUNT_COL_NUM = 19;
	public static int ATO_DATE_COL_NUM = 20;
	public static int GARRISON_THEATER_COL_NUM = 21;
	public static int DEPLOYMENT_COL_NUM = 23;
	public static int IS_MOBILE_COL_NUM = 24;
	public static int SYSTEM_BASED_COL_NUM = 25;
	public static int COTS_PRODUCT_COL_NUM = 26;
	public static int COTS_PRODUCT_NAME_COL_NUM = 27;
	public static int COTS_VENDOR_NAME_COL_NUM = 28;
	public static int COTS_DOD_MODULES_COL_NUM = 29;
	public static int COMMENTS_COL_NUM = 30;

	//List of header caches	
	public static HashMap<String, Integer> PERSONNEL_HEADER_CACHE = null;
	public static HashMap<String, Integer> USER_INTERFACE_HEADER_CACHE = null;
	public static HashMap<String, Integer> ACTIVITY_HEADER_CACHE = null;
	public static HashMap<String, Integer> BUSINESS_PROCESS_HEADER_CACHE = null;
	public static HashMap<String, Integer> BLU_HEADER_CACHE = null;
	public static HashMap<String, Integer> DATA_OBJECT_HEADER_CACHE = null;

	public FormsDataProcessor(){
		
	}
	
	public void processData(IDatabaseEngine engine, File sourceFolder){
		LOGGER.info("SELECTED DATABASE ENGINE::: " + engine.getEngineId());
		LOGGER.info("SELECTED SOURCE FOLDER ::: " + Utility.cleanLogString(sourceFolder.getName()));

		LOGGER.info("The Nonservices marked for review are: ");
		NONSERVICES_REVIEWED_SYSTEMS_LIST = getReviewedSystems(engine, NONSERVICES_REVIEWED_SYSTEMS_QUERY);
		LOGGER.info("The Services marked for review are: ");
		SERVICES_REVIEWED_SYSTEMS_LIST = getReviewedSystems(engine, SERVICES_REVIEWED_SYSTEMS_QUERY);
		
		//processQueries(engine);
		LOGGER.info("************* NONSERVICES_REVIEWED_SYSTEMS_LIST::: " + NONSERVICES_REVIEWED_SYSTEMS_LIST.size());
		if(NONSERVICES_REVIEWED_SYSTEMS_LIST.size() > 0){
			processTaskerSourceFile(engine, sourceFolder);
			processBLUFile(engine, sourceFolder);
			processSiteFile(engine, sourceFolder);
			processSysDataFile(engine, sourceFolder);
			LOGGER.info("********** Begin - updating System Interfaces");
			new FormsICDDataProcessor().processICDFile(engine, sourceFolder, NONSERVICES_REVIEWED_SYSTEMS_LIST);
			LOGGER.info("********** Done - updating System Interfaces");
			//LOGGER.info("********** Begin - updating Site Specific System Interfaces");
			//new FormsSiteICDDataProcessor().processICDFile(engine, sourceFolder, NONSERVICES_REVIEWED_SYSTEMS_LIST);
			//LOGGER.info("********** Done - updating Site Specific System Interfaces");

			//Should not need to un-comment since the data importer will be run off of the pulled database and not the live MESOC version
			//LOGGER.info("********** Begin - updating Status of Reviewed Systems to 'Pushed'");
			//changeSystemsStatus(engine, NONSERVICES_REVIEWED_SYSTEMS_LIST, CHANGE_STATUS_QUERY);
			//LOGGER.info("********** Done - updating Status of Reviewed Systems to 'Pushed'");
		}
		LOGGER.info("************* Finished processData");
	}
	
	private void changeSystemsStatus(IDatabaseEngine engine, ArrayList<String> listToFilterOn, String query){
		final String PROP_URI = "http://semoss.org/ontologies/Relation/Contains/Status";
		final String REVIEW_VALUE = "Reviewed";
		final String PUSHED_VALUE = "Pushed";
		
//		String query = "SELECT DISTINCT ?formSec ?propName ?propValue WHERE {"
//				+ "{?formSec a <http://semoss.org/ontologies/Concept/FormSection>} "
//				+ "BIND(<" + PROP_URI + "> AS ?propName) "
//				+ "BIND(\"" + REVIEW_VALUE + "\" AS ?propValue) "
//				+ "{?formSec ?propName ?propValue} "
//				+ "}";
		
		String localQuery = "";
		String systemsToQueryFor = "";
		//loop through each system and add that to the query
		for (int i = 0; i < listToFilterOn.size(); i++) {
			systemsToQueryFor = systemsToQueryFor + "(<" + SYSTEM_URI + listToFilterOn.get(i) + ">)";
		}
		//make a query with the chosen systems and pull the information for the query
		localQuery = query.replace("@SYSTEM@", systemsToQueryFor);
		
		// list to store the concepts that need the property to be removed
		List<String> formSecUri = new Vector<String>();

		// step 1) only removing the property value
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, localQuery);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] rawValues = row.getRawValues();
				Object[] cleanValues = row.getValues();
				Object[] removeStatement = new Object[4];
				for(int i = 0; i < 3; i++) {
					if(i < 2) {
						removeStatement[i] = rawValues[i] + "";
					} else {
						removeStatement[i] = cleanValues[i] + "";
					}
				}
				// last boolean denotes if the object in the triple is a URI or a literal
				removeStatement[3] = false;
				System.out.println(Arrays.toString(removeStatement));
				// this will remove from forms
				engine.doAction(ACTION_TYPE.REMOVE_STATEMENT, removeStatement);
				
				// keep track of fromSecUri
				// cause we need to this to go from "Reviewed" to "Pushed"
				formSecUri.add(removeStatement[0] + "");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// step 2) add new property value
		for(String formSecUriInstance : formSecUri) {
			Object[] addStatement = new Object[4];
			addStatement[0] = formSecUriInstance;
			addStatement[1] = PROP_URI;
			addStatement[2] = PUSHED_VALUE;
			addStatement[3] = false;
			engine.doAction(ACTION_TYPE.ADD_STATEMENT, addStatement);
		}
		
		engine.commit();
	}
	
	private void processTaskerSourceFile(IDatabaseEngine engine, File sourceFolder){
		LOGGER.info("********** Begin - updating Tasker File");
		try{
			HashMap<String, HashMap<String,String>> map = getSystemInfoForSystems(engine);
			//get all non-service systems that are in reviewed status
			if(map.size() > 0){
				
				String fileName = sourceFolder.getAbsolutePath() + TASKER_FILE;
				XSSFWorkbook wb = getWorkBook(fileName);
		
				//update system information
				LOGGER.info("********** Begin - updating System Information");
				updateSystemInformation(wb, map);
				LOGGER.info("********** Done - updating System Information");
				
				//update personnel
				LOGGER.info("********** Begin - updating System Users - Personnel Tab");
				XSSFSheet personnelSheet = getSheet(wb, PERSONNEL_SHEET_NAME);
				PERSONNEL_HEADER_CACHE = updateHeaderCache(wb, personnelSheet, PERSONNEL_HEADER_CACHE);
				updateFromArrayList(personnelSheet, getDataForNodes(PERSONNEL_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), PERSONNEL_HEADER_CACHE);
				LOGGER.info("********** Done - updating System Users - Personnel Tab");
				
				//update UI
				LOGGER.info("********** Begin - updating System User Interfaces - UI Tab");
				XSSFSheet uiSheet = getSheet(wb, USER_INTERFACE_SHEET_NAME);
				USER_INTERFACE_HEADER_CACHE = updateHeaderCache(wb, uiSheet, USER_INTERFACE_HEADER_CACHE);
				updateFromArrayList(uiSheet, getDataForNodes(USER_INTERFACE_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), USER_INTERFACE_HEADER_CACHE);
				LOGGER.info("********** Done - updating System User Interfaces - UI Tab");
				
				//update activity
				LOGGER.info("********** Begin - updating Activities - Activities Tab");
				XSSFSheet activitySheet = getSheet(wb, ACTIVITY_SHEET_NAME);
				ACTIVITY_HEADER_CACHE = updateHeaderCache(wb, activitySheet, ACTIVITY_HEADER_CACHE);
				updateFromArrayList(activitySheet, getDataForNodes(ACTIVITY_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), ACTIVITY_HEADER_CACHE);
				LOGGER.info("********** Done - updating Activities - Activities Tab");
				
			
				//update business processes
				LOGGER.info("********** Begin - updating Business Processes - BP Tab");
				XSSFSheet bpSheet = getSheet(wb, BUSINESS_PROCESS_SHEET_NAME);
				BUSINESS_PROCESS_HEADER_CACHE = updateHeaderCache(wb, bpSheet, BUSINESS_PROCESS_HEADER_CACHE);
				updateFromArrayList(bpSheet, getDataForNodes(BUSINESS_PROCESS_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), BUSINESS_PROCESS_HEADER_CACHE);
				LOGGER.info("********** Done - updating Business Processes - BP Tab");
				
				//Update Software
				LOGGER.info("********** Begin - updating System Software - Software Tab");
				new FormsSWDataProcessor().updateData(wb, getDataForTables(SOFTWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
				LOGGER.info("********** Done - updating System Software - Software Tab");
				
				//Update Hardware
				LOGGER.info("********** Begin - updating System Hardware - Hardware Tab");
				//tableToString(getDataForTables(HARDWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
				new FormsHWDataProcessor().updateData(wb, getDataForTables(HARDWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
				LOGGER.info("********** Done - updating System Hardware - Hardware Tab");
				
				Utility.writeWorkbook(wb, fileName);
			}
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
		LOGGER.info("********** Done - updating Tasker File");		
	}
	
	private void processBLUFile(IDatabaseEngine engine, File sourceFolder){
		LOGGER.info("********** Begin - updating System BLU");
		try{
			//LOGGER.info("********** Querying for BLU");
			HashMap<String, HashMap<String,String>> map = getSystemInfoForSystems(engine);
			//get all non-service systems that are in reviewed status
			if(map.size() > 0){
				
				String fileName = sourceFolder.getAbsolutePath() + BLU_FILE;
				XSSFWorkbook wb = getWorkBook(fileName);

				//update blu
				XSSFSheet bluSheet = getSheet(wb, BLU_SHEET_NAME);
				BLU_HEADER_CACHE = updateHeaderCache(wb, bluSheet, BLU_HEADER_CACHE);
				updateFromArrayList(bluSheet, getDataForNodes(BLU_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), BLU_HEADER_CACHE);

				Utility.writeWorkbook(wb, fileName);
			}
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
		LOGGER.info("********** Done - updating System BLU");
	}

	private void processSiteFile(IDatabaseEngine engine, File sourceFolder){
		LOGGER.info("********** Begin - updating System Deployments");
		try{
			//LOGGER.info("********** Querying for Site");
			HashMap<String, HashMap<String,String>> map = getSystemInfoForSystems(engine);
			//get all non-service systems that are in reviewed status
			if(map.size() > 0){
				
				String fileName = sourceFolder.getAbsolutePath() + SITE_FILE;
				XSSFWorkbook wb = getWorkBook(fileName);

				//update DC Site
				XSSFSheet dcSiteSheet = getSheet(wb, DCSITE_SHEET_NAME);
				updateFromSite(dcSiteSheet, getDataForNodes(DCSITE_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST));

				//update mtf
				XSSFSheet mtfSheet = getSheet(wb, MTF_SHEET_NAME);
				updateFromSite(mtfSheet, getDataForNodes(MTF_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST));

				//processSysTestsiteStateNetwork(wb, getSystemInfoForSystems(engine));

				Utility.writeWorkbook(wb, fileName);
			}
			
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
		LOGGER.info("********** Done - updating System Deployments");
		
	}

//	private void processSysTestsiteStateNetwork(XSSFWorkbook wb, HashMap<String, HashMap<String,String>> systemInfo) {
//		//for each system in NONSERVICES_REVIEWED_SYSTEMS_LIST
//		//	get the sys, ts, state, network, method from the hashmap
//		//		call updateSysTSStateNetwork for .COMMil
//		//		call updateSysTSStateNetwork for .
//	}
//
//	private void updateSysTestsiteStateNetwork(wb, sys, ts, net as strings) {
//		//string state = "AsIs"
//		//get first sheet from wb
//		//deletesystestsitestatenet(asdfasd f)
//		//add our value to the bottom
//		//
//		//get second sheet from wb
//		//deletesystestsitestatenet(asdfasd f)
//		//add our value to the bottom
//	}
//
//	private void deletesystestsitestatenet(sheet, col, int place1, String val1, int place2, String val2) {
//
//	}
	
	private void processSysDataFile(IDatabaseEngine engine, File sourceFolder){
		LOGGER.info("********** Begin - updating Data Objects");
		try{
			//LOGGER.info("********** Querying for System Interfaces");
			HashMap<String, HashMap<String, HashMap<String,String>>> map = getDataForTables(DATA_OBJECT_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
			//LOGGER.info("********** interfaces size: " + map.size());
			if(map.size() > 0){
							
				//tableToString(map);
				String fileName = sourceFolder.getAbsolutePath() + SYS_DATA_FILE;
				XSSFWorkbook wb = getWorkBook(fileName);
				XSSFSheet dataObjectSheet = getSheet(wb, DATA_OBJECT_SHEET_NAME);
				
				DATA_OBJECT_HEADER_CACHE = updateHeaderCache(wb, dataObjectSheet, DATA_OBJECT_HEADER_CACHE);
				updateFromDataObject(dataObjectSheet, map, DATA_OBJECT_HEADER_CACHE);
				
				Utility.writeWorkbook(wb, fileName);
			}
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
		LOGGER.info("********** Done - updating Data Objects");
	}

	private void updateSystemInformation(XSSFWorkbook wb, HashMap<String, HashMap<String,String>> sysInfoMap)throws IOException{

		XSSFSheet lSheet = getSheet(wb, SYSTEM_INFORMATION_SHEET_NAME);
		
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		boolean systemFound = false;
		
		for (String key : sysInfoMap.keySet()) {
			int count = 0;
		    for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
		    	count = rIndex;
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure row is not null
				if (row != null) {
					XSSFCell systemNameCell = row.getCell(1);
					if (systemNameCell != null ) {
						// get the name of the System from the sheet
						String systemName = systemNameCell.getStringCellValue();
						// Find the row you are looking for
						if (systemName.equals(key)) {
							LOGGER.info("systemName: " + systemName);
							HashMap<String,String> valueMap = sysInfoMap.get(key);
						    for (String vKey : valueMap.keySet()) {
						    	//LOGGER.info("vKey = " + vKey);
						    	//LOGGER.info("********Value = " + valueMap.get(vKey));
						    	int n = getSysInfoColumnNumber(vKey);
						    	//LOGGER.info(" ************* n = " + n);
						    	if(n!=-1){
						    		XSSFCell cell = row.getCell(n);
									cell.setCellValue(valueMap.get(vKey));
						    	}
						    }
						    systemFound = true;
						    break;
						}
					}
				}
			}
		    if(!systemFound){
		    	LOGGER.info(" ************* System Not Found - Creating a row in SystemInformation Tab for: " + Utility.cleanLogString(key));
		    	//system not found, create a new row
		    	
				XSSFRow row = lSheet.createRow(count++);
				XSSFCell cell = row.createCell(1);
				cell.setCellValue(key);
				
				HashMap<String,String> valueMap = sysInfoMap.get(key);
			    for (String vKey : valueMap.keySet()) {
			    	int n = getSysInfoColumnNumber(vKey);
			    	//LOGGER.info(" ************* n = " + n);
			    	if(n!=-1){
			    		XSSFCell c = row.createCell(n);
						c.setCellValue(valueMap.get(vKey));
			    	}
			    }
			}
		}
	}
	
	private int getSysInfoColumnNumber(String name){
		switch (name)
	    {
	      case "FullSystemName":
	    	  return FULL_SYSTEM_NAME_COL_NUM;
	      case "Description":
	    	  return DESCRIPTION_COL_NUM;
	      case "NumberofUsers":
	    	  return NUM_OF_USERS_COL_NUM;
	      case "UserConsoles":
	    	  return USER_CONSOLES_COL_NUM;
	      case "AvailabilityRequired":
	    	  return AVAILABILITY_REQUIRED_COL_NUM;
	      case "AvailabilityActual":
	    	  return AVAILABILITY_ACTUAL_COL_NUM;
	      case "EndofSupportDate":
	    	  return END_OF_SUPPORT_DATE_COL_NUM;
	      case "TransactionCount":
	    	  return TRANSACTION_COUNT_COL_NUM;
	      case "ATODate":
	    	  return ATO_DATE_COL_NUM;
	      case "GarrisonTheater":
	    	  return GARRISON_THEATER_COL_NUM;
		  case "DeploymentType":
		  	  return DEPLOYMENT_COL_NUM;
		  case "IsMobile":
		  	  return IS_MOBILE_COL_NUM;
		  case "SystemBased":
		  	  return SYSTEM_BASED_COL_NUM;
		  case "COTSProduct":
		  	  return COTS_PRODUCT_COL_NUM;
		  case "COTSProductName":
		  	  return COTS_PRODUCT_NAME_COL_NUM;
		  case "COTSVendorName":
		  	  return COTS_VENDOR_NAME_COL_NUM;
		  case "COTSDoDModules":
		  	  return COTS_DOD_MODULES_COL_NUM;
		  case "Comments":
		  	  return COMMENTS_COL_NUM;
	      default:
	          return -1;
	    }
	}

	private void updateFromArrayList(XSSFSheet lSheet, HashMap<String, ArrayList<String>> map, HashMap<String, Integer> headerCache)throws IOException{
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		for (int i=0; i<NONSERVICES_REVIEWED_SYSTEMS_LIST.size(); i++) {
		String key = NONSERVICES_REVIEWED_SYSTEMS_LIST.get(i);
		//for (String key : map.keySet()) {
			//LOGGER.info("lastRow: " + lastRow);
			int count = 0;
			
		    for (int rIndex = 0; rIndex <= lastRow; rIndex++) {
		    	count = rIndex;
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure row is not null
				if (row != null) {
					XSSFCell systemNameCell = row.getCell(0);
					if (systemNameCell != null ) {
						// get the name of the System from the sheet
						String systemName = systemNameCell.getStringCellValue();
						// Find the row you are looking for
						if (systemName.equals(key)) {
							lSheet.removeRow(row);
						    break;
						}
					}
				}
			}


		    //increment the last row if we're adding a row to the bottom
		    if (count == lastRow) {
		    	LOGGER.info("System not found, adding to end of sheet");
		    	++lastRow;
		    }

	    	LOGGER.info("Deleting old row and creating new one for: " + key);
	    	//system not found, create a new row
			XSSFRow row = lSheet.createRow(count++);
			XSSFCell cell = row.createCell(0);
			cell.setCellValue(key);
			
			if(map.get(key) != null) {
				ArrayList<String> valueList = map.get(key);
			    for (int j=0; j<valueList.size(); j++) {
					Integer n = null;
					String cleanedString = trimSpecialCharacters(valueList.get(j));
					//if the value exists in the cleaned header
					if(headerCache.get(cleanedString)!=null) {
						n = headerCache.get(cleanedString);
						if(n.intValue()!=0){
							XSSFCell c = row.createCell(n.intValue());
							c.setCellValue("X");
						}
					} 
					else {
						LOGGER.info("Could not find " + cleanedString + " in sheet");
					}
				}
			}
		}
	}

	private void updateFromSite(XSSFSheet lSheet, HashMap<String, ArrayList<String>> map)throws IOException{
		// remove applicable rows
		int lastRow = lSheet.getLastRowNum();
		for (int i=0; i<NONSERVICES_REVIEWED_SYSTEMS_LIST.size(); i++) {
			String key = NONSERVICES_REVIEWED_SYSTEMS_LIST.get(i);
			//LOGGER.info("Beginning row deletion");
			//LOGGER.info("lastRow: " + lastRow);
			
		    for (int rIndex = 0; rIndex <= lastRow; rIndex++) {
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure row is not null
				if (row != null) {
					XSSFCell systemNameCell = row.getCell(0);
					if (systemNameCell != null ) {
						// get the name of the System from the sheet
						String systemName = systemNameCell.getStringCellValue();
						// Find the row you are looking for
						if (systemName.equals(key)) {
							lSheet.removeRow(row);
						}
					}
				}
			}
			
			//removeRowsForSystem(lSheet, key, 0);
			
			if(map.get(key) != null) {
				ArrayList<String> valueList = map.get(key);
			    for (int j=0; j<valueList.size(); j++) {
					//create a new row
					XSSFRow row = lSheet.createRow(lastRow);
					++lastRow;

					//set column 0 value
			    	//LOGGER.info("key: " + key);
					XSSFCell cell = row.createCell(0);
					cell.setCellValue(key);

					//set column 1 value
					//LOGGER.info("value: " + valueList.get(j));
					XSSFCell c = row.createCell(1);
					c.setCellValue(valueList.get(j));
				}
			}
		}
		
		//LOGGER.info("************* End update Site");
	}
	
	private void updateFromDataObject(XSSFSheet lSheet, HashMap<String, HashMap<String, HashMap<String,String>>> hashmap1, HashMap<String, Integer> headerCache)throws IOException{
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		for (int i=0; i<NONSERVICES_REVIEWED_SYSTEMS_LIST.size(); i++) {
		String key = NONSERVICES_REVIEWED_SYSTEMS_LIST.get(i);
		//for (String key : map.keySet()) {
			//LOGGER.info("lastRow: " + lastRow);
			int count = 0;
		    for (int rIndex = 0; rIndex <= lastRow; rIndex++) {
		    	count = rIndex;
				XSSFRow row = lSheet.getRow(rIndex);
				// check to make sure row is not null
				if (row != null) {
					XSSFCell systemNameCell = row.getCell(0);
					if (systemNameCell != null ) {
						// get the name of the System from the sheet
						String systemName = systemNameCell.getStringCellValue();
						// Find the row you are looking for
						if (systemName.equals(key)) {
							lSheet.removeRow(row);
						    break;
						}
					}
				}
			}

		    //increment the last row if we're adding a row to the bottom
		    if (count == lastRow) {
		    	LOGGER.info("System not found, adding to end of sheet");
		    	++lastRow;
		    }

	    	LOGGER.info("Deleting old row and creating new one for: " + key);
	    	//system not found, create a new row
			XSSFRow row = lSheet.createRow(count++);
			XSSFCell cell = row.createCell(0);
			cell.setCellValue(key);
			
			if(hashmap1.get(key) != null) {
				HashMap<String, HashMap<String, String>> hashmap2 = hashmap1.get(key);
			    for (String key2 : hashmap2.keySet()) {

					//LOGGER.info("vKey = " + vKey);
					//LOGGER.info("********Value = " + valueMap.get(vKey));
					//LOGGER.info("values: ");
					//LOGGER.info(key2);
					
					Integer n = null;
					String cleanedString = trimSpecialCharacters(key2);
					//if the value exists in the cleaned header
					if(headerCache.get(cleanedString)!=null) {
						n = headerCache.get(cleanedString);
						//LOGGER.info(" ************* n = " + n);
						if(n.intValue()!=0){
							HashMap<String, String> hashmap3 = hashmap2.get(key2);
							XSSFCell c = row.createCell(n.intValue());
							//set the value equal to the value of the first key of Hashmap3
							if (hashmap3.get("CRM") != null) {
								c.setCellValue(hashmap3.get("CRM"));
							}
						}
					} 
					else {
						LOGGER.info("Could not find " + Utility.cleanLogString(cleanedString) + " in sheet");
					}
				}
			}
		}
		
		//LOGGER.info("************* End update from Data Object");
	}

}
