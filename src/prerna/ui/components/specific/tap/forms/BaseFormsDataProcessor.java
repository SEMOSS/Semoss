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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.ui.components.specific.tap.QueryProcessor;
import prerna.util.DIHelper;
import prerna.util.Utility;

//UNDER DEVELOPMENT - CLASS NOT USED AT THIS TIME
public class BaseFormsDataProcessor {
	public static final Logger LOGGER = LogManager.getLogger(BaseFormsDataProcessor.class.getName());

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();


	//query for grabbing the reviewed systems
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
	public static String SYSTEM_INFORMATION_QUERY = "SELECT DISTINCT ?System ?FullSystemName ?Description ?NumberofUsers ?UserConsoles ?AvailabilityActual ?AvailabilityRequired ?TransactionCount ?ATODate ?GarrisonTheater ?EndofSupportDate ?DeploymentType ?IsMobile ?SystemBased ?COTSProduct ?COTSProductName ?COTSVendorName ?COTSDoDModules ?Comments WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/AvailabilityActual> ?AvailabilityActual} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired> ?AvailabilityRequired} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Full_System_Name> ?FullSystemName} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NumberofUsers} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/User_Consoles> ?UserConsoles} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?TransactionCount} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATODate} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GarrisonTheater} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?EndofSupportDate} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/DeploymentType> ?DeploymentType} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/IsMobile> ?IsMobile} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/SystemBased> ?SystemBased} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/COTS_Product> ?COTSProduct} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/COTS_Vendor_Name> ?COTSVendorName} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/COTS_DoD_Modules> ?COTSDoDModules} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/COTS_Product_Name> ?COTSProductName} OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Comments> ?Comments} }BINDINGS ?System {@SYSTEM@}";
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
	//public static String SYSTEM_INTERFACE_ENTERPRISE_QUERY = "SELECT DISTINCT ?System ?SystemInterface ?Provider ?Consumer ?DataObject ?Format ?Frequency ?Protocol WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?DataObject} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq> ;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Protocol} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Format} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Frequency} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Provider <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Consumer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} MINUS{ {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite} } {BIND(?System as ?Provider)} UNION{ BIND(?System as ?Consumer)} {?Provider <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Consumer} } BINDINGS ?System {@SYSTEM@}";
	//query for site specific level system interfaces. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	//public static String SYSTEM_INTERFACE_SITE_SPECIFIC_QUERY = "SELECT DISTINCT ?System ?SystemInterface ?DCSite ?Provider ?Consumer ?DataObject ?Format ?Frequency ?Protocol WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq> ;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?DataObject} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Protocol} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Format} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Frequency} {?SystemInterface <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Provider <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Consumer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {BIND(?System as ?Provider)} UNION{ BIND(?System as ?Consumer)} {?Provider <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Consumer} } BINDINGS ?System {@SYSTEM@}";
	
	//queries all of system interface, including DC Site which is optional
	public static String SYSTEM_INTERFACE_QUERY = "SELECT DISTINCT ?System ?SystemInterface ?DCSite ?Provider ?Consumer ?DataObject ?Format ?Frequency ?Protocol ?Trigger WHERE { {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt> ;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq> ;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm> ;} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?DataObject} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Protocol} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Format} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?Frequency} OPTIONAL{{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;}{?SystemInterface <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite;}} OPTIONAL{{?Trigger <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?SystemInterface <http://semoss.org/ontologies/Relation/Triggers> ?Trigger;}} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Provider <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Consumer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {BIND(?System as ?Provider)} UNION{ BIND(?System as ?Consumer)} {?Provider <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Consumer} } BINDINGS ?System {@SYSTEM@}"; 
	//query for software and relevant software information. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String SOFTWARE_QUERY = "SELECT DISTINCT ?System ?SystemSoftwareVersion ?Software ?Version ?Quantity ?TotalCost WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemSoftwareVersion> ;} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?SystemSoftwareVersion} {?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Software> ?Software} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Version> ?Version} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} OPTIONAL{?SystemSoftwareVersion <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost} } BINDINGS ?System {@SYSTEM@}";
	//query for hardware and relevant hardware information. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String HARDWARE_QUERY = "SELECT DISTINCT ?System ?SystemHardwareModel ?Hardware ?Model ?Quantity ?TotalCost ?ProductType WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemHardwareModel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemHardwareModel> ;} {?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Hardware> ?Hardware} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Model> ?Model} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TotalCost} OPTIONAL{?SystemHardwareModel <http://semoss.org/ontologies/Relation/Contains/ProductType> ?ProductType} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?SystemHardwareModel} } BINDINGS ?System {@SYSTEM@}";
	//query for MTF (where system is available) and DMIS ID. Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String MTF_QUERY = "SELECT DISTINCT ?System ?MTF WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF> ;} {?System <http://semoss.org/ontologies/Relation/AvailableAt> ?MTF} } BINDINGS ?System {@SYSTEM@}";
	//query for DCSites (installation Sites where system is hosted). Has the ability to take in any number of systems to filter on. Must have a list to filter on though
	public static String DCSITE_QUERY = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite> ;} {?System <http://semoss.org/ontologies/Relation/HostedAt> ?DCSite} } BINDINGS ?System {@SYSTEM@}";
	//query for the status changing	
	public static String CHANGE_STATUS_QUERY = "SELECT DISTINCT ?FormSection ?propName ?Status WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?FormSection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FormSection>} {?System <http://semoss.org/ontologies/Relation/Has> ?FormSection} BIND(<http://semoss.org/ontologies/Relation/Contains/Status> AS ?propName) {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>} {?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Air_Force>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Navy>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/Army>} MINUS{?System <http://semoss.org/ontologies/Relation/OwnedBy> <http://health.mil/ontologies/Concept/SystemOwner/NCR-MD>} {?FormSection <http://semoss.org/ontologies/Relation/Contains/Status> ?Status} FILTER(?Status IN (\"Reviewed\")) } BINDINGS ?System {@SYSTEM@}";
	
	//the uri needed when designating a system
	public static final String SYSTEM_URI = "http://health.mil/ontologies/Concept/System/";

	public BaseFormsDataProcessor(){
		
	}
	
	protected HashMap<String, Integer> updateHeaderCache(XSSFWorkbook wb, XSSFSheet lSheet, HashMap<String, Integer> headerCache)throws IOException{
		return updateHeaderCache(wb, lSheet, headerCache, 1);
	}
		
	//update the corresponding HEADER_CACHE to contain all of the strings from the first row of the wb
	protected HashMap<String, Integer> updateHeaderCache(XSSFWorkbook wb, XSSFSheet lSheet, HashMap<String, Integer> headerCache, int count)throws IOException{
		return updateHeaderCache(wb, lSheet, headerCache, count, -1);
		/*
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
				count++;
			}
			LOGGER.info("************* headerCache size: " + headerCache.size());
		}
		LOGGER.info("************* End updateHeaderCache for " + lSheet.getSheetName());
		return headerCache;
		*/
	}
	
	//update the corresponding HEADER_CACHE to contain all of the strings from the first row of the wb
	protected HashMap<String, Integer> updateHeaderCache(XSSFWorkbook wb, XSSFSheet lSheet, HashMap<String, Integer> headerCache, int startingCol, int endingCol)throws IOException{
		LOGGER.info("************* Begin updateHeaderCache for " + lSheet.getSheetName());
		//LOGGER.info("************* startingCol: " + startingCol + " endingCol: " + endingCol);
		
		if (headerCache == null){
			headerCache = new HashMap<String, Integer>();
			XSSFRow headerRow = lSheet.getRow(0);
			if(endingCol == -1){
				int count = startingCol;
				while (headerRow.getCell(count)!=null && 
						headerRow.getCell(count).getStringCellValue()!=null && 
						!headerRow.getCell(count).getStringCellValue().equals("")){
					String cleanedString = trimSpecialCharacters(headerRow.getCell(count).getStringCellValue());
					headerCache.put(cleanedString, new Integer(count));
					count++;
				}
			}
			else {
				for(int i=startingCol; i<=endingCol; i++){
					//LOGGER.info("************* i: " + i );
					String cleanedString = trimSpecialCharacters(headerRow.getCell(i).getStringCellValue());
					//LOGGER.info("************* Adding cleanedString: " + cleanedString + " ColNum: " + i);
					headerCache.put(cleanedString, new Integer(i));				
				}
			}
			LOGGER.info("************* headerCache size: " + headerCache.size());
		}
		LOGGER.info("************* End updateHeaderCache for " + lSheet.getSheetName());
		return headerCache;
	}

	protected int getColNumFromCache(String value, HashMap<String, Integer> headerCache){
		String cleanedString = trimSpecialCharacters(value);
		if(headerCache.get(cleanedString)!=null) {
	    	int n = headerCache.get(cleanedString);
	    	return n;
	    } 
    	else {
    		LOGGER.info("Could not find " + cleanedString + " in sheet");
    	}
		return -1;
	}
	
	protected String trimSpecialCharacters(String str){
		//LOGGER.info("************* Begin trimSpecialCharacters: " + str);
		str = str.replaceAll(" ", "");
		str = str.replaceAll("-", "");
		str = str.replaceAll("_", "");
		str = str.replaceAll("/", "");
		//LOGGER.info("************* End trimSpecialCharacters: " + str);
		return str;
	}
	
	protected void removeRowsForSystem(XSSFSheet lSheet, String systemName, int systemNameColNum){
		//LOGGER.info("********* in removeRowsForSystem ");
		int lastRow = lSheet.getLastRowNum();
		//LOGGER.info("********* lastRow: " + lastRow);
		for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
	    	//LOGGER.info("********* rIndex: " + rIndex);
			XSSFRow row = lSheet.getRow(rIndex);
			// check to make sure row is not null
			if (row != null) {
				XSSFCell systemCell1 = row.getCell(systemNameColNum);
				if (systemCell1 != null) {
					// get the name of the System from the sheet
					String system1Name = systemCell1.getStringCellValue();
					// Find the row you are looking for
					if (system1Name.equals(systemName)) {
						//LOGGER.info("********* system1Name: " + system1Name + " system2Name: " + system2Name + "********* Removing row");
						lSheet.removeRow(row);
						//LOGGER.info("********* lastRow: " + lastRow + " rIndex: " + rIndex);
						//lSheet.shiftRows(rIndex+1, lastRow, -1);
						//rIndex = rIndex-1;
						//LOGGER.info("********* lastRow after removing: " + lSheet.getLastRowNum());
					}
				}
			}
		}
	}
	
	protected XSSFSheet getSheet(XSSFWorkbook wb, String sheetName) throws IOException {
		XSSFSheet lSheet = wb.getSheet(sheetName);
		if (lSheet == null) {
			throw new IOException("Could not find Loader Sheet in Excel file " + sheetName);
		}
		return lSheet;
	}

	protected XSSFWorkbook getWorkBook(String fileName) throws FileNotFoundException, IOException {
		LOGGER.info("************** Begin getWorkBook for " + Utility.cleanLogString(fileName));
		
		
		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
			
		} catch (FileNotFoundException e) {
			//LOGGER.info("hit FNF Exception");
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			//LOGGER.info("hit IO Exception");
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			if(e.getMessage()!= null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			//LOGGER.info("hit finally");
			if(poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					LOGGER.info("hit IO Exception");
					e.printStackTrace();
					//throw new IOException("Could not close Excel file stream");
				}
			}
			
			try{
				if(workbook!=null)
					workbook.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		LOGGER.info("************** End getWorkBook");
		return workbook;
	}
	
	protected ArrayList<String> getReviewedSystems(IDatabaseEngine engine, String queryToRun){
		//get the list of reviewed systems
		ArrayList<String> listToPopulate = QueryProcessor.getStringList(queryToRun, engine.getEngineId());
		LOGGER.info(listToPopulate.toString());
		return listToPopulate;
	}
	
	protected HashMap<String, HashMap<String,String>> getSystemInfoForSystems(IDatabaseEngine engine){
		HashMap<String, HashMap<String,String>> systemInfoMap = queryDataForProperties(SYSTEM_INFORMATION_QUERY, NONSERVICES_REVIEWED_SYSTEMS_LIST, engine);
		LOGGER.info("The info pulled for system information is: " + systemInfoMap.toString());
		return systemInfoMap;
	}

	/*
	 * A method for calling the needed information based on the declared queries
	 * Could potentially make this more generic to take in a list of queries, engines, and what the output should be
	 * param: engine - the engine that the data is pulled from (is a database)
	 */
	// private void processQueries(IDatabase engine) {
	// 	System.out.println("***** Loading system information ****");
	// 	//get the list of reviewed systems
	// 	REVIEWED_SYSTEMS_LIST = QueryProcessor.getStringList(REVIEWED_SYSTEMS_QUERY, engine.getEngineName());
	// 	System.out.println("The systems marked for review are: " + REVIEWED_SYSTEMS_LIST.toString());
		
	// 	//1. query the information for system information and print it out
	// 	HashMap<String, HashMap<String,String>> systemInfoMap = queryDataForProperties(SYSTEM_INFORMATION_QUERY, REVIEWED_SYSTEMS_LIST, engine);
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
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> dataObjectMapping = FormsDataProcessor.getDataForTables(DATA_OBJECT_QUERY, REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(dataObjectMapping);
	// 	//8. query the information for enterprise level system interfaces and print them out
	// 	System.out.println("-------Enterprise Level System Interface Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> systemInterfaceEnterpriseMapping = FormsDataProcessor.getDataForTables(SYSTEM_INTERFACE_ENTERPRISE_QUERY, REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(systemInterfaceEnterpriseMapping);
	// 	//9. query the information for site specific system interfaces and print them out
	// 	System.out.println("-------Site Specific Level System Interface Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> systemInterfaceSiteSpecificMapping = FormsDataProcessor.getDataForTables(SYSTEM_INTERFACE_SITE_SPECIFIC_QUERY, REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(systemInterfaceSiteSpecificMapping);
	// 	//10. query the information for system software and print it out
	// 	System.out.println("-------Software Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> softwareTableMapping = FormsDataProcessor.getDataForTables(SOFTWARE_QUERY, REVIEWED_SYSTEMS_LIST, engine);
	// 	FormsDataProcessor.tableToString(softwareTableMapping);
	// 	//11. query the information for system hardware and print it out
	// 	System.out.println("-------Hardware Info:");
	// 	HashMap<String, HashMap<String, HashMap<String, String>>> hardwareTableMapping = FormsDataProcessor.getDataForTables(HARDWARE_QUERY, REVIEWED_SYSTEMS_LIST, engine);
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
	protected static HashMap<String, HashMap<String,String>> queryDataForProperties(String query, ArrayList<String> listToFilterOn, IDatabaseEngine engine) {
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
		consolidatedMapping = getMapOfStringMapOfStrings(localQuery, engine.getEngineId());
		//LOGGER.info("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}
	
	/*
	 * Method for grabbing values from a query that only pertain to a list of values to bind on. It runs a query for each value in the listToFilterOn
	 * This could be changed to one pull if the proper way to have multiple parameters in SPARQL is figured out
	 * param query: A string that contains the query that will be pulled
	 * param listToFilterOn: The arraylist of values to use to filter for. Each value replaces the generic bind value in the query
	 * param engine: The database to pull the data from
	 */
	protected static HashMap<String, ArrayList<String>> getDataForNodes(String query, IDatabaseEngine engine, ArrayList<String> listToFilterOn) {
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
		consolidatedMapping = QueryProcessor.getStringListMap(localQuery, engine.getEngineId());
		LOGGER.info("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}

	/*
	 * Method 
	 * @param query: A string that contains the query that will be pulled
	 * @param listToFilterOn: The arraylist of values to use to filter for. Each value replaces the generic bind value in the query
	 * @param engine: The database to pull the data from
	 * @return 
	 */
	protected static HashMap<String, HashMap<String, HashMap<String,String>>> getDataForTables(String query, ArrayList<String> listToFilterOn, IDatabaseEngine engine) {
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
		consolidatedMapping = getMapOfTable(localQuery, engine.getEngineId());
		LOGGER.info("Adding this to the map: " + consolidatedMapping.toString());

		return consolidatedMapping;
	}

	// /**
	//  * Take two maps and merge them into one map
	//  * @param  HashMap<String,String>>> map1 map to merge with
	//  * @param  HashMap<String,String>>> map2 other map to merge with, this one will be overwritten
	//  * @return HashMap<String,String>>> the merged map
	//  */
	// public static HashMap<String, HashMap<String, HashMap<String,String>>> mergeMaps(HashMap<String, HashMap<String, HashMap<String,String>>>mapA, HashMap<String, HashMap<String, HashMap<String,String>>>mapB) {
	// 	LOGGER.info("Map A: ");
	// 	tableToString(mapA);
	// 	LOGGER.info("Map B: ");
	// 	tableToString(mapB);

	// 	HashMap<String, HashMap<String, HashMap<String,String>>> mapC = mapB;
	// 	mapC.putAll(mapA);
	// 	for (String key: mapC.keySet()){
	// 		if (mapB.containsKey(key)) {
	// 			HashMap<String, HashMap<String,String>> mapB1 = mapB.get(key);
	// 			mapC.get(key).putAll(mapB1);
	// 		}
	// 		if (mapA.containsKey(key)) {
	// 			HashMap<String, HashMap<String,String>> mapA1 = mapA.get(key);
	// 			mapC.get(key).putAll(mapA1);
	// 		}
	// 	}
	// 	LOGGER.info("Map C: ");
	// 	tableToString(mapC);
	// 	return mapC;
	// }
	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap with a
	 * string and hashmap inside of it and prints it out in a readable format
	 * param mapToPrint: The hasmap to be printed
	 */
	public static void mapOfMapStrStrToString(HashMap<String, HashMap<String,String>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int count = 0;
		//loop through each outer key and print
		for (String outerKey: mapToPrint.keySet()){
			count++;
            String key = outerKey.toString();
            LOGGER.info(Utility.cleanLogString("Outer key #" + count + " is: " + key));  
            //loop through each inner key and print the key + value
            for (String innerKey: mapToPrint.get(key).keySet()) {
            	String key1 = innerKey.toString();
            	LOGGER.info(Utility.cleanLogString(">>>>>>" + key1 + " = " + mapToPrint.get(key).get(key1)));
            }
            LOGGER.info("");
		} 
	}
	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap with a
	 * string and hashmap inside of it and prints it out in a readable format
	 * param mapToPrint: The hasmap to be printed
	 */
	public static void mapOfMapStrArrListToString(HashMap<String, ArrayList<String>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int count = 0;
		//loop through each outer key and print
		for (String key: mapToPrint.keySet()){
			count++;
			//counter to count the values for printing
			int countValue = 0;
            String keyString = key.toString();
            LOGGER.info(Utility.cleanLogString("Key #" + count + " is: " + key));  
            //loop through each inner key and print the key + value
            for (String value: mapToPrint.get(key)) {
            	String valueString = value.toString();
            	LOGGER.info(Utility.cleanLogString(">>>>>>Value #" + countValue + " = " + valueString));
            	countValue++;
            }
            LOGGER.info("");
		} 
	}
	
	/*
	 * Method for printing out the contents of a map. This takes the Hashmap that emulates a multi-column table and prints it out
	 * param mapToPrint: The hasmap to be printed
	 */
	public static void tableToString(HashMap<String, HashMap<String,HashMap<String,String>>> mapToPrint) {
		//create a count to know the number key for printing purposes
		int firstcount = 0;
		//loop through each outer key and print
		for (String firstKey: mapToPrint.keySet()){
			int secondcount = 0;
			firstcount++;
            String firstkeyString = firstKey.toString();
            LOGGER.info(Utility.cleanLogString("Outer key #" + firstcount + " is: " + firstkeyString));  
            //loop through each inner key and print the key + value
            
            for (String secondKey: mapToPrint.get(firstkeyString).keySet()) {
            	secondcount++;
            	String secondkeyString = secondKey.toString();
            	LOGGER.info(Utility.cleanLogString("   Inner Key #" + secondcount + " for " + firstkeyString + " is " + secondkeyString));
            	for (String thirdKey: mapToPrint.get(firstkeyString).get(secondkeyString).keySet()) {
                	String thirdkeyString = thirdKey.toString();
                	LOGGER.info(Utility.cleanLogString("      >>>" + thirdkeyString + " = " + mapToPrint.get(firstkeyString).get(secondkeyString).get(thirdkeyString)));
                }
            	
            }
		} 
	}	
	
	/*
	 * Method for changing the system status from reviewed to pushed
	 * param reviewedList: The list of systems to change the status for
	 * param engine: the database to change the statuses in
	 */
	protected void changeStatus(ArrayList<String> reviewedList, IDatabaseEngine engine) {
		//TODO: Talk to Maher about changing a property in a database. Wrote down a method he's used in FormBuilder
//		engine.doAction(IDatabase.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{nodeURI, propURI, propVal, false});
	}
	
	
	/*
	 * Queries for data and returns a hashmap that has a key and an arraylist
	 * param query: The query to be run
	 * param engine: the database to change the statuses in
	 */
	protected static HashMap<String, HashMap<String, String>> getMapOfStringMapOfStrings(String query, String engineName) {
		//creates a map to store the pulled values in. This is returned
		HashMap<String, HashMap<String, String>> finalMap = new HashMap<String, HashMap<String, String>>();
		try {
			//here is where the data get pulled from the BE
			IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(engineName);
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

	/*
	 * Queries for data and returns a hashmap that has a key and hashmap( key, hashmap(key value))
	 * param query: The query to be run
	 * param engine: the database to change the statuses in
	 */
	protected static HashMap<String, HashMap<String, HashMap<String, String>>> getMapOfTable(String query, String engineName) {
		//creates a map to store the pulled values in. This is returned
		HashMap<String, HashMap<String, HashMap<String, String>>> finalMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();
		try {
			//here is where the data get pulled from the BE
			IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			String[] values = sjsw.getVariables();
			//loop through each row returned
			while (sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				//create placeholders for the inner hashmaps
				HashMap<String, HashMap<String, String>> tempBigMap = new HashMap<String, HashMap<String, String>>();
				HashMap<String, String> tempSmallMap = new HashMap<String, String>();
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
				} else {
					int i = 1;
					String newSecondKey = secondKey;
					while (finalMap.get(firstKey).containsKey(newSecondKey)) {
						LOGGER.info("Hit duplicate value for " + newSecondKey);
						newSecondKey = secondKey + "%" + i;
						i++;
					}
					secondKey = newSecondKey;
					//LOGGER.info("Adding <" + firstKey + ", <" + secondKey + ", < , >>> to FinalMap");
					finalMap.get(firstKey).put(secondKey, tempSmallMap);
					//LOGGER.info("You are repeating a value that should be unique. Occuring in:" + firstKey + " ,"+ secondKey);
				}
				//here is the change not in the QueryProcessor.java file
				//loops through the column headers from the query and stores everything for the row (except the key) in an arraylist
				for(int i = 2; i < values.length; i++) {
					if (!sjss.getVar(values[i]).toString().isEmpty()) {
						//for each column in the specific MAIN key, add a new key value pair. The new key is the column header and the value is the value for the column
						finalMap.get(firstKey).get(secondKey).put(values[i], sjss.getVar(values[i]).toString());
					}
				}
				
			}
			//TODO: Make this catch more robust. Could have an issue with arrayindex out of bounds based on user inputting a bad query
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: " + engineName);
		}
		return finalMap;
	}
	
}
