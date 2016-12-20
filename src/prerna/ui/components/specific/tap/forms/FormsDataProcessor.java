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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class FormsDataProcessor extends BaseFormsDataProcessor{
	public static final Logger LOGGER = LogManager.getLogger(FormsDataProcessor.class.getName());

	//List of files
	public static final String TASKER_FILE = "\\Tasker Responses Collection Worksheet.xlsx";
	public static final String BLU_FILE = "\\System BLU.xlsx";
	public static final String SITE_FILE = "\\TAP_Site_Data-Loader.xlsm";
	public static final String ICD_FILE = "\\ICD Consolidated to Service Mapping_Validated.xlsm";
	public static final String SITE_ICD_FILE = "\\ICD SiteSpecific.xlsm";
	public static final String SYS_DATA_FILE = "\\System Data.xlsx";
	
	public static final String SYSTEM_INFORMATION_SHEET_NAME = "System_Information";
	public static final String PERSONNEL_SHEET_NAME = "System_to_Personnel";
	public static final String USER_INTERFACE_SHEET_NAME = "System-UI";
	public static final String ACTIVITY_SHEET_NAME = "System_to_Activity";
	public static final String BUSINESS_PROCESS_SHEET_NAME = "System_to_BP";
	public static final String BLU_SHEET_NAME = "System - BLU";
	public static final String DCSITE_SHEET_NAME = "System-To-Deployment";
	public static final String MTF_SHEET_NAME = "System-To-Availability";
	public static final String DATA_OBJECT_SHEET_NAME = "System-Data";		

	public static int AVAILABILITY_ACTUAL_COL_NUM = 7;
	public static int AVAILABILITY_REQUIRED_COL_NUM = 6;
	public static int DESCRIPTION_COL_NUM = 3;
	public static int END_OF_SUPPORT_DATE_COL_NUM = 11;
	public static int USER_CONSOLES_COL_NUM = 5;
	public static int FULL_SYSTEM_NAME_COL_NUM = 2;
	public static int NUM_OF_USERS_COL_NUM = 4;
	public static int TRANSACTION_COUNT_COL_NUM = 8;
	public static int ATO_DATE_COL_NUM = 9;
	public static int GARRISON_THEATER_COL_NUM = 10;
	public static int DEPLOYMENT_COL_NUM = 25;
	public static int IS_MOBILE_COL_NUM = 26;
	public static int SYSTEM_BASED_COL_NUM = 27;

	//List of header caches	
	public static HashMap<String, Integer> PERSONNEL_HEADER_CACHE = null;
	public static HashMap<String, Integer> USER_INTERFACE_HEADER_CACHE = null;
	public static HashMap<String, Integer> ACTIVITY_HEADER_CACHE = null;
	public static HashMap<String, Integer> BUSINESS_PROCESS_HEADER_CACHE = null;
	public static HashMap<String, Integer> BLU_HEADER_CACHE = null;
	public static HashMap<String, Integer> DATA_OBJECT_HEADER_CACHE = null;
	
	
/*	//query for grabbing the reviewed systems
	public static final String REVIEWED_SYSTEMS_QUERY = "SELECT DISTINCT ?System WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?FormSection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FormSection> ;} {?System <http://semoss.org/ontologies/Relation/Has> ?FormSection} {?FormSection <http://semoss.org/ontologies/Relation/Contains/Status> ?Status} FILTER(?Status IN (\"Reviewed\")) }";
	//query for grabbing the reviewed systems that ARE service systems
	public static final String SERVICES_REVIEWED_SYSTEMS_QUERY = "SELECT DISTINCT ?System WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?FormSection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FormSection>} {?System <http://semoss.org/ontologies/Relation/Has> ?FormSection} {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>} {?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner} {?FormSection <http://semoss.org/ontologies/Relation/Contains/Status> ?Status} FILTER(?Status IN (\"Reviewed\")) } BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)(<http://health.mil/ontologies/Concept/SystemOwner/NCR-MD>)}";
	//query for grabbing the reviewed systems that are non-service systems
	public static final String NONSERVICES_REVIEWED_SYSTEMS_QUERY = "SELECT DISTINCT ?System WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?FormSection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FormSection>} {?System <http://semoss.org/ontologies/Relation/Has> ?FormSection} {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>} {?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Air_Force>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Navy>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Army>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/NCR-MD>} {?FormSection <http://semoss.org/ontologies/Relation/Contains/Status> ?Status} FILTER(?Status IN (\"Reviewed\")) }";
	//lists of reviewed NON-SERVICE systems to be used throughout the file
	public static ArrayList<String> NONSERVICES_REVIEWED_SYSTEMS_LIST = new ArrayList<String>();
	//lists of reviewed SERVICE systems to be used throughout the file
	public static ArrayList<String> SERVICES_REVIEWED_SYSTEMS_LIST = new ArrayList<String>();
	//Query for system information. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String SYSTEM_INFORMATION_QUERY = "SELECT DISTINCT ?System ?FullSystemName ?Description ?NumberofUsers ?UserConsoles ?AvailabilityActual ?AvailabilityRequired ?TransactionCount ?ATODate ?GarrisonTheater ?EndofSupportDate ?DeploymentType ?IsMobile ?SystemBased WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/AvailabilityActual> ?AvailabilityActual} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired> ?AvailabilityRequired} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Full_System_Name> ?FullSystemName} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NumberofUsers} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/User_Consoles> ?UserConsoles} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?TransactionCount} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATODate} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GarrisonTheater} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?EndofSupportDate} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/DeploymentType> ?DeploymentType} {?SystemHardware <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemHardware> ;} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?SystemHardware} OPTIONAL{?SystemHardware <http://semoss.org/ontologies/Relation/Contains/IsMobile> ?IsMobile} OPTIONAL{?SystemHardware <http://semoss.org/ontologies/Relation/Contains/SystemBased> ?SystemBased} }BINDINGS ?System {@SYSTEM@}";
	//query for personnel. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String PERSONNEL_QUERY = "SELECT DISTINCT ?System ?Personnel WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel> ;} {?System <http://semoss.org/ontologies/Relation/UsedBy> ?Personnel} } BINDINGS ?System {@SYSTEM@}";
	//query for user interfaces. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String USER_INTERFACE_QUERY = "SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?UserInterface} } BINDINGS ?System {@SYSTEM@}";
	//query for business processes. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String BUSINESS_PROCESS_QUERY = "SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess} } BINDINGS ?System {@SYSTEM@}";
	//query for activities. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String ACTIVITY_QUERY = "SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?Activity} } BINDINGS ?System {@SYSTEM@}"; 
	//query for business logic units. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String BLU_QUERY = "SELECT DISTINCT ?System ?BusinessLogicUnit WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?BusinessLogicUnit} } BINDINGS ?System {@SYSTEM@}";
	//query for data Objects. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String DATA_OBJECT_QUERY = "SELECT DISTINCT ?System ?DataObject ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemDataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDataObject> ;} {?System <http://semoss.org/ontologies/Relation/PartOf> ?SystemDataObject} {?SystemDataObject <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM} BIND(URI(CONCAT('http://semoss.org/ontologies/Concept/SystemDataObject/',STRAFTER(STR(?SystemDataObject), \"%\"))) as ?DataObject) } BINDINGS ?System {@SYSTEM@}";
	//query for enterprise level system interfaces. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String SYSTEM_INTERFACE_ENTERPRISE_QUERY = "SELECT DISTINCT ?System ?SystemInterface ?Provider ?Consumer ?DataObject ?Format ?Frequency ?Protocol WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?DataObject} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq> ;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Protocol} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Format} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Frequency} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Provider <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Consumer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {BIND(?System as ?Provider)} UNION{ BIND(?System as ?Consumer)} {?Provider <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Consumer} } BINDINGS ?System {@SYSTEM@}";
	//query for site specific level system interfaces. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String SYSTEM_INTERFACE_SITE_SPECIFIC_QUERY = "SELECT DISTINCT ?System ?SystemInterface ?DCSite ?Provider ?Consumer ?DataObject ?Format ?Frequency ?Protocol WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq> ;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?DataObject} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Protocol} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Format} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Frequency} {?SystemInterface <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Provider <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Consumer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {BIND(?System as ?Provider)} UNION{ BIND(?System as ?Consumer)} {?Provider <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Consumer} } BINDINGS ?System {@SYSTEM@}";
	//query for software and relevant software information. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String SOFTWARE_QUERY = "SELECT DISTINCT ?System ?SystemSoftwareVersion ?Software ?Version ?Quantity ?TotalCost WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemSoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemSoftwareVersion> ;} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?SystemSoftwareVersion} {?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Software> ?Software} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Version> ?Version} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost} } BINDINGS ?System {@SYSTEM@}";
	//query for hardware and relevant hardware information. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String HARDWARE_QUERY = "SELECT DISTINCT ?System ?SystemHardwareModel ?Hardware ?Model ?Quantity ?TotalCost ?ProductType WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemHardwareModel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemHardwareModel> ;} {?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Hardware> ?Hardware} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Model> ?Model} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/ProductType> ?ProductType} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?SystemHardwareModel} } BINDINGS ?System {@SYSTEM@}";
	//query for MTF (where system is available) and DMIS ID. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String MTF_QUERY = "SELECT DISTINCT ?System ?MTF WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF> ;} {?System <http://semoss.org/ontologies/Relation/AvailableAt> ?MTF} } BINDINGS ?System {@SYSTEM@}";
	//query for DCSites (installation Sites where system is hosted). Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String DCSITE_QUERY = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?System <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite} } BINDINGS ?System {@SYSTEM@}";
			
	//the uri needed when designating a system
	public static final String SYSTEM_URI = "http://health.mil/ontologies/Concept/System/";
*/
	public FormsDataProcessor(){
		
	}
	
	public void processData(IEngine engine, File sourceFolder){
		LOGGER.info("SELECTED DATABASE ENGINE::: " + engine.getEngineName());
		LOGGER.info("SELECTED SOURCE FOLDER ::: " + sourceFolder.getName());

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
			new FormsICDDataProcessor().processICDFile(engine, sourceFolder, NONSERVICES_REVIEWED_SYSTEMS_LIST);
			new FormsSiteICDDataProcessor().processICDFile(engine, sourceFolder, NONSERVICES_REVIEWED_SYSTEMS_LIST);
			changeSystemsStatus(engine, NONSERVICES_REVIEWED_SYSTEMS_LIST, CHANGE_STATUS_QUERY);
		}
		LOGGER.info("************* Finished processData");
	}
	
	private void changeSystemsStatus(IEngine engine, ArrayList<String> listToFilterOn, String query){
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
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, localQuery);
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
	
	private void processTaskerSourceFile(IEngine engine, File sourceFolder){
		try{
			//LOGGER.info("********** Querying for SysInfo");
			HashMap<String, HashMap<String,String>> map = getSystemInfoForSystems(engine);
			//get all non-service systems that are in reviewed status
			if(map.size() > 0){
				
				String fileName = sourceFolder.getAbsolutePath() + TASKER_FILE;
				XSSFWorkbook wb = getWorkBook(fileName);
		
				//update system information
				updateSystemInformation(wb, map);
				
				//update personnel
				XSSFSheet personnelSheet = getSheet(wb, PERSONNEL_SHEET_NAME);
				PERSONNEL_HEADER_CACHE = updateHeaderCache(wb, personnelSheet, PERSONNEL_HEADER_CACHE);
				updateFromArrayList(personnelSheet, getDataForNodes(PERSONNEL_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), PERSONNEL_HEADER_CACHE);
			
				//update UI
				XSSFSheet uiSheet = getSheet(wb, USER_INTERFACE_SHEET_NAME);
				USER_INTERFACE_HEADER_CACHE = updateHeaderCache(wb, uiSheet, USER_INTERFACE_HEADER_CACHE);
				updateFromArrayList(uiSheet, getDataForNodes(USER_INTERFACE_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), USER_INTERFACE_HEADER_CACHE);
				
				//update activity
				XSSFSheet activitySheet = getSheet(wb, ACTIVITY_SHEET_NAME);
				ACTIVITY_HEADER_CACHE = updateHeaderCache(wb, activitySheet, ACTIVITY_HEADER_CACHE);
				updateFromArrayList(activitySheet, getDataForNodes(ACTIVITY_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), ACTIVITY_HEADER_CACHE);

			
				//update business processes
				XSSFSheet bpSheet = getSheet(wb, BUSINESS_PROCESS_SHEET_NAME);
				BUSINESS_PROCESS_HEADER_CACHE = updateHeaderCache(wb, bpSheet, BUSINESS_PROCESS_HEADER_CACHE);
				updateFromArrayList(bpSheet, getDataForNodes(BUSINESS_PROCESS_QUERY, engine, NONSERVICES_REVIEWED_SYSTEMS_LIST), BUSINESS_PROCESS_HEADER_CACHE);

				//Update Software
				new FormsSWDataProcessor().updateData(wb, 
						getDataForTables(SOFTWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
			
				//Update Hardware
				//tableToString(getDataForTables(HARDWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
				new FormsHWDataProcessor().updateData(wb, 
						getDataForTables(HARDWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine));
				
				Utility.writeWorkbook(wb, fileName);
			}
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
	}
	
	private void processBLUFile(IEngine engine, File sourceFolder){
		try{
			LOGGER.info("********** Querying for BLU");
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
	}

	private void processSiteFile(IEngine engine, File sourceFolder){
		try{
			LOGGER.info("********** Querying for Site");
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

				Utility.writeWorkbook(wb, fileName);
			}
			
		} catch (Exception e) {
			LOGGER.info("Error");
			e.printStackTrace();
		} 
	}
	
	private void processSysDataFile(IEngine engine, File sourceFolder){
		try{
			LOGGER.info("********** Querying for System Interfaces");
			HashMap<String, HashMap<String, HashMap<String,String>>> map = 
					getDataForTables(DATA_OBJECT_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
			//LOGGER.info("********** interfaces size: " + map.size());
			if(map.size() > 0){
							
				tableToString(map);
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
		    	LOGGER.info(" ************* System Not Found - Creating a row in SystemInformation Tab for: " + key);
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
			LOGGER.info("lastRow: " + lastRow);
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
					//LOGGER.info("vKey = " + vKey);
					//LOGGER.info("********Value = " + valueMap.get(vKey));
					//LOGGER.info("values:");
					//LOGGER.info(valueList.get(j));
					
					Integer n = null;
					String cleanedString = trimSpecialCharacters(valueList.get(j));
					//if the value exists in the cleaned header
					if(headerCache.get(cleanedString)!=null) {
						n = headerCache.get(cleanedString);
						//LOGGER.info(" ************* n = " + n);
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
			LOGGER.info("Beginning row deletion");
			LOGGER.info("lastRow: " + lastRow);
			
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
		
		LOGGER.info("************* End update Site");
	}
	
	private void updateFromDataObject(XSSFSheet lSheet, HashMap<String, HashMap<String, HashMap<String,String>>> hashmap1, HashMap<String, Integer> headerCache)throws IOException{
		// determine number of rows to load
		int lastRow = lSheet.getLastRowNum();
		for (int i=0; i<NONSERVICES_REVIEWED_SYSTEMS_LIST.size(); i++) {
		String key = NONSERVICES_REVIEWED_SYSTEMS_LIST.get(i);
		//for (String key : map.keySet()) {
			LOGGER.info("lastRow: " + lastRow);
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
					LOGGER.info("values: ");
					LOGGER.info(key2);
					
					Integer n = null;
					String cleanedString = trimSpecialCharacters(key2);
					//if the value exists in the cleaned header
					if(headerCache.get(cleanedString)!=null) {
						n = headerCache.get(cleanedString);
						LOGGER.info(" ************* n = " + n);
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
						LOGGER.info("Could not find " + cleanedString + " in sheet");
					}
				}
			}
		}
		
		LOGGER.info("************* End update from Data Object");
	}

	//update the corresponding HEADER_CACHE to contain all of the strings from the first row of the wb
/*	protected HashMap<String, Integer> updateHeaderCache(XSSFWorkbook wb, XSSFSheet lSheet, 
			HashMap<String, Integer> headerCache)throws IOException{
		return updateHeaderCache(wb, lSheet, headerCache, 1);
	}
		
	//update the corresponding HEADER_CACHE to contain all of the strings from the first row of the wb
	protected HashMap<String, Integer> updateHeaderCache(XSSFWorkbook wb, XSSFSheet lSheet, HashMap<String, Integer> headerCache, int count)throws IOException{
		LOGGER.info("************* Begin updateHeaderCache for " + lSheet.getSheetName());
		
		if (headerCache == null){
			headerCache = new HashMap<String, Integer>();
			XSSFRow headerRow = lSheet.getRow(0);
			//int count = 1;
			while (headerRow.getCell(count)!=null && 
			headerRow.getCell(count).getStringCellValue()!=null && 
			!headerRow.getCell(count).getStringCellValue().equals("")){
				String cleanedString = trimSpecialCharacters(headerRow.getCell(count).getStringCellValue());
				headerCache.put(cleanedString, new Integer(count));
				//LOGGER.info("************* Adding " + cleanedString  + " to HEADER_CACHE");
				count++;
			}
			LOGGER.info("************* headerCache size: " + headerCache.size());
		}
		LOGGER.info("************* End updateHeaderCache for " + lSheet.getSheetName());
		return headerCache;
	}

	protected String trimSpecialCharacters(String str){
		//LOGGER.info("************* Begin trimSpecialCharacters: " + str);
		str = str.replaceAll(" ", "");
		str = str.replaceAll("-", "");
		str = str.replaceAll("_", "");
		//LOGGER.info("************* End trimSpecialCharacters: " + str);
		return str;
	}
	
	protected XSSFSheet getSheet(XSSFWorkbook wb, String sheetName) throws IOException {
		XSSFSheet lSheet = wb.getSheet(sheetName);
		if (lSheet == null) {
			throw new IOException("Could not find Loader Sheet in Excel file " + sheetName);
		}
		return lSheet;
	}

	protected XSSFWorkbook getWorkBook(String fileName) throws FileNotFoundException, IOException {
		LOGGER.info("************** Begin getWorkBook for " + fileName);
		
		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			LOGGER.info("hitting poi reader");
			poiReader = new FileInputStream(fileName);
			LOGGER.info("hitting workbook");
			workbook = new XSSFWorkbook(poiReader);
			LOGGER.info("finishing try block");

		} catch (FileNotFoundException e) {
			LOGGER.info("hit FNF Exception");
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			LOGGER.info("hit IO Exception");
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			LOGGER.info("hit Exception");
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			LOGGER.info("hit finally");
			if(poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					LOGGER.info("hit IO Exception");
					e.printStackTrace();
					throw new IOException("Could not close Excel file stream");
				}
			}
		}
		LOGGER.info("************** End getWorkBook");
		return workbook;
	}
	
	private ArrayList<String> getReviewedSystems(IEngine engine, String queryToRun){
		//get the list of reviewed systems
		ArrayList<String> listToPopulate = QueryProcessor.getStringList(queryToRun, engine.getEngineName());
		System.out.println(listToPopulate.toString());
		return listToPopulate;
	}
	
	private HashMap<String, HashMap<String,String>> getSystemInfoForSystems(IEngine engine){
		HashMap<String, HashMap<String,String>> systemInfoMap = queryDataForProperties(SYSTEM_INFORMATION_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
		System.out.println("The info pulled for system information is: " + systemInfoMap.toString());
		return systemInfoMap;
	}
*/
	/*
	 * A method for calling the needed information based on the declared queries
	 * Could potentially make this more generic to take in a list of queries, engines, and what the output should be
	 * param: engine - the engine that the data is pulled from (is a database)
	 */
	// private void processQueries(IEngine engine) {
	// 	System.out.println("***** Loading system information ****");
	// 	//get the list of reviewed systems
	// 	NONSERVICES_REVIEWED_SYSTEMS_LIST = QueryProcessor.getStringList(REVIEWED_SYSTEMS_QUERY, engine.getEngineName());
	// 	System.out.println("The systems marked for review are: " + NONSERVICES_REVIEWED_SYSTEMS_LIST.toString());
		
	// 	//1. query the information for system information and print it out
	// 	HashMap<String, HashMap<String,String>> systemInfoMap = queryDataForProperties(SYSTEM_INFORMATION_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.mapOfMapStrStrToString(systemInfoMap);
		
		
	// 	System.out.println("***** Loading node information ****");
	// 	//TODO: This wont return systems that have no data for personnel, we need to make sure we still delete the data for those rows in the db
		
	// 	//2. query the information for personnel and print it out
	// 	System.out.println("-------Personnel Info:");
	// 	HashMap<String, ArrayList<String>> personnelMapping = getDataForNodes(PERSONNEL_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(personnelMapping);
	// 	//3. query the information for user interfaces and print it out
	// 	System.out.println("-------User Interface Info:");
	// 	HashMap<String, ArrayList<String>> userInterfaceMapping = getDataForNodes(USER_INTERFACE_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(userInterfaceMapping);
	// 	//4. query the information for business processes and print it out
	// 	System.out.println("-------Business Process Info:");
	// 	HashMap<String, ArrayList<String>> businessProcessMapping = getDataForNodes(BUSINESS_PROCESS_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(businessProcessMapping);
	// 	//5. query the information for activities and print it out
	// 	System.out.println("-------Activity Info:");
	// 	HashMap<String, ArrayList<String>> activityMapping = getDataForNodes(ACTIVITY_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(activityMapping);
	// 	//6. query the information for business logic units (BLU) and print it out
	// 	System.out.println("-------BLU Info:");
	// 	HashMap<String, ArrayList<String>> businessLogicUnitMapping = getDataForNodes(BUSINESS_LOGIC_UNIT_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(businessLogicUnitMapping);
	// 	//7. query the information for data objects (DO) and print it out
	// 	System.out.println("-------Data Object Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> dataObjectMapping = FormsDataProcessor.getDataForTables(DATA_OBJECT_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(dataObjectMapping);
	// 	//8. query the information for enterprise level system interfaces and print them out
	// 	System.out.println("-------Enterprise Level System Interface Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> systemInterfaceEnterpriseMapping = FormsDataProcessor.getDataForTables(SYSTEM_INTERFACE_ENTERPRISE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(systemInterfaceEnterpriseMapping);
	// 	//9. query the information for site specific system interfaces and print them out
	// 	System.out.println("-------Site Specific Level System Interface Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> systemInterfaceSiteSpecificMapping = FormsDataProcessor.getDataForTables(SYSTEM_INTERFACE_SITE_SPECIFIC_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(systemInterfaceSiteSpecificMapping);
	// 	//10. query the information for system software and print it out
	// 	System.out.println("-------Software Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> softwareTableMapping = FormsDataProcessor.getDataForTables(SOFTWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(softwareTableMapping);
	// 	//11. query the information for system hardware and print it out
	// 	System.out.println("-------Hardware Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> hardwareTableMapping = FormsDataProcessor.getDataForTables(HARDWARE_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(hardwareTableMapping);
	// 	//12. query the mtf and dmis id mapping for the system and print it out
	// 	System.out.println("-------MTF Info:");
	// 	HashMap<String, ArrayList<String>> mtfMapping = getDataForNodes(MTF_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(mtfMapping);
	// 	//13. query the information for user interfaces and print it out
	// 	System.out.println("-------DC Site Info:");
	// 	HashMap<String, ArrayList<String>> dcSiteMapping = getDataForNodes(DCSITE_QUERY, engine);
	// 	FormsDataProcessor.mapOfMapStrArrListToString(dcSiteMapping);
		
		
	// }
	
	/*
	 * Method for grabbing values from a query that only pertain to a list of values to bind on. It runs a query for each value in the listToFilterOn
	 * This could be changed to one pull if the proper way to have multiple parameters in SPARQL is figured out
	 * param query: A string that contains the query that will be pulled
	 * param listToFilterOn: The arraylist of values to use to filter for. Each value replaces the generic bind value in the query
	 * param engine: The database to pull the data from
	 */
	/*private static HashMap<String, HashMap<String,String>> queryDataForProperties(String query, ArrayList<String> listToFilterOn, IEngine engine) {
		//create a hashmap to return and store the data pulled for chosen systems
		HashMap<String, HashMap<String,String>> consolidatedMapping = new HashMap<String, HashMap<String,String>>();
		String localQuery = "";
		
		String systemsToQueryFor = "";
		//loop through each system and add that to the query
		for (int i = 0; i < listToFilterOn.size(); i++) {
			systemsToQueryFor = systemsToQueryFor + "(<" + SYSTEM_URI + listToFilterOn.get(i) + ">)";
		}
		//make a query with the chosen systems and pull the information for the query
		localQuery = query.replace("@SYSTEM@", systemsToQueryFor);
		consolidatedMapping = getMapOfStringMapOfStrings(localQuery, engine.getEngineName());
		System.out.println("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}*/
	
	/*
	 * Method for grabbing values from a query that only pertain to a list of values to bind on. It runs a query for each value in the listToFilterOn
	 * This could be changed to one pull if the proper way to have multiple parameters in SPARQL is figured out
	 * param query: A string that contains the query that will be pulled
	 * param listToFilterOn: The arraylist of values to use to filter for. Each value replaces the generic bind value in the query
	 * param engine: The database to pull the data from
	 */
	/*private static HashMap<String, ArrayList<String>> getDataForNodes(String query, IEngine engine, ArrayList<String> listToFilterOn) {
		//create a hashmap to return and store the data pulled for chosen systems
		HashMap<String, ArrayList<String>> consolidatedMapping = new HashMap<String, ArrayList<String>>();
		String localQuery = "";
		String systemsToQueryFor = "";
		//loop through each system and add that to the query
		for (int i = 0; i < listToFilterOn.size(); i++) {
			systemsToQueryFor = systemsToQueryFor + "(<" + SYSTEM_URI + listToFilterOn.get(i) + ">)";
		}
		//make a query with the chosen systems and pull the information for the query
		localQuery = query.replace("@SYSTEM@", systemsToQueryFor);
		consolidatedMapping = QueryProcessor.getStringListMap(localQuery, engine.getEngineName());
		System.out.println("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}*/

	/*
	 * Method 
	 * param query: A string that contains the query that will be pulled
	 * param listToFilterOn: The arraylist of values to use to filter for. Each value replaces the generic bind value in the query
	 * param engine: The database to pull the data from
	 */
	/*protected static HashMap<String, HashMap<String, HashMap<String,String>>> getDataForTables(String query, ArrayList<String> listToFilterOn, IEngine engine) {
		//create a hashmap to return and store the data pulled for chosen systems
		HashMap<String, HashMap<String, HashMap<String, String>>> consolidatedMapping = new HashMap<String, HashMap<String, HashMap<String, String>>>();
		String localQuery = "";
		
		String systemsToQueryFor = "";
		//loop through each system and add that to the query
		for (int i = 0; i < listToFilterOn.size(); i++) {
			systemsToQueryFor = systemsToQueryFor + "(<" + SYSTEM_URI + listToFilterOn.get(i) + ">)";
		}
		//make a query with the chosen systems and pull the information for the query
		localQuery = query.replace("@SYSTEM@", systemsToQueryFor);
		consolidatedMapping = FormsDataProcessor.getMapOfTable(localQuery, engine.getEngineName());
		System.out.println("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}*/

	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap with a
	 * string and hashmap inside of it and prints it out in a readable format
	 * param mapToPrint: The hasmap to be printed
	 */
	/*public static void mapOfMapStrStrToString(HashMap<String, HashMap<String,String>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int count = 0;
		//loop through each outer key and print
		for (String outerKey: mapToPrint.keySet()){
			count++;
            String key = outerKey.toString();
            System.out.println("Outer key #" + count + " is: " + key);  
            //loop through each inner key and print the key + value
            for (String innerKey: mapToPrint.get(key).keySet()) {
            	String key1 = innerKey.toString();
            	System.out.println(">>>>>>" + key1 + " = " + mapToPrint.get(key).get(key1));
            }
            System.out.println("");
		} 
	}*/
	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap with a
	 * string and hashmap inside of it and prints it out in a readable format
	 * param mapToPrint: The hasmap to be printed
	 */
	/*public static void mapOfMapStrArrListToString(HashMap<String, ArrayList<String>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int count = 0;
		//loop through each outer key and print
		for (String key: mapToPrint.keySet()){
			count++;
			//counter to count the values for printing
			int countValue = 0;
            String keyString = key.toString();
            System.out.println("Key #" + count + " is: " + key);  
            //loop through each inner key and print the key + value
            for (String value: mapToPrint.get(key)) {
            	String valueString = value.toString();
            	System.out.println(">>>>>>Value #" + countValue + " = " + valueString);
            	countValue++;
            }
            System.out.println("");
		} 
	}*/
	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap that emulates a multi-column table and prints it out
	 * param mapToPrint: The hasmap to be printed
	 */
	/*public static void tableToString(HashMap<String, HashMap<String,HashMap<String,String>>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int firstcount = 0;
		//loop through each outer key and print
		for (String firstKey: mapToPrint.keySet()){
			int secondcount = 0;
			firstcount++;
            String firstkeyString = firstKey.toString();
            System.out.println("Outer key #" + firstcount + " is: " + firstkeyString);  
            //loop through each inner key and print the key + value
            
            for (String secondKey: mapToPrint.get(firstkeyString).keySet()) {
            	secondcount++;
            	String secondkeyString = secondKey.toString();
            	System.out.println("   Inner Key #" + secondcount + " for " + firstkeyString + " is " + secondkeyString);
            	for (String thirdKey: mapToPrint.get(firstkeyString).get(secondkeyString).keySet()) {
                	String thirdkeyString = thirdKey.toString();
                	System.out.println("      >>>" + thirdkeyString + " = " + mapToPrint.get(firstkeyString).get(secondkeyString).get(thirdkeyString));
                }
            	
            }
            System.out.println("");
		} 
	}	
	*/
	/*
	 * Method for changing the system status from reviewed to pushed
	 * param reviewedList: The list of systems to change the status for
	 * param engine: the database to change the statuses in
	 */
/*	private void changeStatus(ArrayList<String> reviewedList, IEngine engine) {
		//TODO: Talk to Maher about changing a property in a database. Wrote down a method he's used in FormBuilder
//		engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, propURI, propVal, false});
	}*/
	
	
	/*
	 * Queries for data and returns a hashmap that has a key and an arraylist
	 * param query: The query to be run
	 * param engine: the database to change the statuses in
	 */
	/*private static HashMap<String, HashMap<String, String>> getMapOfStringMapOfStrings(String query, String engineName) {
		//creates a map to store the pulled values in. This is returned
		HashMap<String, HashMap<String, String>> finalMap = new HashMap<String, HashMap<String, String>>();
		try {
			//here is where the data get pulled from the BE
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			//loop through each row returned
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				HashMap<String, String> temp;
				//grabs the value in the first column and stores it as the key of the hashmap
				String key = sjss.getVar(values[0]).toString();
				if (!finalMap.containsKey(key)) {
					temp = new HashMap<String, String>();
					finalMap.put(key, temp);
				}
				//here is the change not in the QueryProcessor.java file
				//loops through the column headers from the query and stores everything for the row (except the key) in an arraylist
				for(int i = 1; i < values.length; i++) {
					//for each column in the specific MAIN key, add a new key value pair. The new key is the column header and the value is the value for the column
					finalMap.get(key).put(values[i], sjss.getVar(values[i]).toString());
				}
				
			}
			//TODO: Make this catch more robust. Could have an issue with arrayindex out of bounds based on user inputting a bad query
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
*/
	/*
	 * Queries for data and returns a hashmap that has a key and hashmap( key, hashmap(key value))
	 * param query: The query to be run
	 * param engine: the database to change the statuses in
	 */
	/*private static HashMap<String, HashMap<String, HashMap<String, String>>> getMapOfTable(String query, String engineName) {
		//creates a map to store the pulled values in. This is returned
		HashMap<String, HashMap<String, HashMap<String, String>>> finalMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();
		try {
			//here is where the data get pulled from the BE
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			//loop through each row returned
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				//create placeholders for the inner hashmaps
				HashMap<String, HashMap<String, String>> tempBigMap = new HashMap<String, HashMap<String, String>>();;
				HashMap<String, String> tempSmallMap = new HashMap<String, String>();;
				//grabs the value in the first column and stores it as the key of the hashmap
				String firstKey = sjss.getVar(values[0]).toString();
				String secondKey = sjss.getVar(values[1]).toString();
				//checks if this outermost key is already in the overall hashmap
				//not in: adds the key and the second level key, which is the second column returned
				//yes in: doesn't add the outermost key, but adds the second level key and a placeholder hashmap to the HM of the outermost key
				if (!finalMap.containsKey(firstKey)) {
					tempBigMap.put(secondKey, tempSmallMap);
					finalMap.put(firstKey, tempBigMap);
					//check if the secondKey is already in the map, if not then add it in
				} else if (!finalMap.get(firstKey).containsKey(secondKey)) {
					finalMap.get(firstKey).put(secondKey, tempSmallMap);
					
				} else {
					System.out.println("You are repeating a value that should be unique. Occuring in:" + firstKey + " ,"+ secondKey);
				}
				//here is the change not in the QueryProcessor.java file
				//loops through the column headers from the query and stores everything for the row (except the key) in an arraylist
				for(int i = 2; i < values.length; i++) {
					//for each column in the specific MAIN key, add a new key value pair. The new key is the column header and the value is the value for the column
					finalMap.get(firstKey).get(secondKey).put(values[i], sjss.getVar(values[i]).toString());
				}
				
			}
			//TODO: Make this catch more robust. Could have an issue with arrayindex out of bounds based on user inputting a bad query
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}	*/
	
}
