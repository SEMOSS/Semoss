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
package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.specific.FactSheetProcessor;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DHMSMDispositionFactSheetProcessor extends FactSheetProcessor {
	private static final Logger logger = LogManager.getLogger(DHMSMDispositionFactSheetProcessor.class.getName());
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	
	private String proposedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedSystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ";
	private String decommissionedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedDecommissionedSystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } }";
	private String allPresentICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ORDER BY ?Interface";
	private final String lpiSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Device> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'}{?entity <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))}";
	private final String lpniSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Device> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPNI'}{?entity <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))}";
	private final String highSystemsQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Device> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Disposition> 'High'}{?entity <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))}";
	private String referenceRepositoryQuery = "SELECT DISTINCT ?DataObject WHERE{BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) BIND(<http://health.mil/ontologies/Concept/SourceType/MigrationReference> AS ?MigrationReference){?MigrationReference <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SourceType>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?DataObjectSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource>;}{?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?DataObjectSource <http://semoss.org/ontologies/Relation/LabeledAs> ?MigrationReference}{?System <http://semoss.org/ontologies/Relation/Designated> ?DataObjectSource}{?DataObjectSource <http://semoss.org/ontologies/Relation/Delivers> ?DataObject}}";
	private String rtmQuery = "SELECT DISTINCT ?RTM_Justification WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/RTM_Justification> ?RTM_Justification ;} }";
	//System and DHMSM Providing BLU Queries
	private String sysNumBLUQuery = "SELECT DISTINCT (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides) WHERE{SELECT DISTINCT ?System ?blu WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu}}} GROUP BY ?System";
	private String sysAndDHMSMNumBLUQuery = "SELECT DISTINCT (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities) WHERE{SELECT DISTINCT ?System ?blu WHERE { BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?System) BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?blu}{?System ?provideBLU ?blu}}} GROUP BY ?System";
	
	private final String sysBaseURI = "http://health.mil/ontologies/Concept/System/";
	
	private double percentDataCreatedDHMSMProvide = 0.0, percentBLUCreatedDHMSMProvide = 0.0;
	
	private IDatabaseEngine TAP_Core_Data;
	private IDatabaseEngine TAP_Cost_Data;
	private IDatabaseEngine TAP_Site_Data;
	private IDatabaseEngine FutureDB;
	private IDatabaseEngine FutureCostDB;
	
	ArrayList<String> systemList = new ArrayList<String>();
	ArrayList<String> lpiSystemList = new ArrayList<String>();
	ArrayList<String> lpniSystemList = new ArrayList<String>();
	ArrayList<String> highSystemList = new ArrayList<String>();	

	public DHMSMDispositionFactSheetProcessor() throws IOException{
		TAP_Cost_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost_Data==null) {
			throw new IOException("TAP_Cost_Data database not found");
		}
		TAP_Core_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		if(TAP_Core_Data==null) {
				throw new IOException("TAP_Core_Data database not found");
		}
		FutureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
		if(FutureDB==null) {
			throw new IOException("FutureDB database not found");
		}
		FutureCostDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureCostDB");
		if(FutureCostDB==null) {
				throw new IOException("FutureCostDB database not found");
		}
	}
	
	
	public boolean retrieveDatabases() {
		boolean success = true;
		try{
			TAP_Core_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(TAP_Core_Data==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report.");
			return false;
		}
		
		try{
			FutureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
			if(FutureDB==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: FutureDB. Cannot generate report.");
			return false;
		}
		return success;
	}
	
	
	@Override
	public void generateReports() {	
		boolean dbcheck = retrieveDatabases();
		if (!dbcheck) {
			return;
		}
		//make this the query to return all High, LPNI, and LPI system
		systemList.addAll(lpiSystemList);
		systemList.addAll(lpniSystemList);
		systemList.addAll(highSystemList);
		boolean shouldStart = true;
		for (int i=0; i<systemList.size(); i++) {
			String systemName = systemList.get(i);
			if(systemName.equals("LDM"))
				shouldStart = true;
			if(shouldStart)
			{
				generateSystemReport(systemName, false);
			}
		}		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\FactSheets\\");		
	}
	
	/**
	 * Processes and stores the fact sheet queries and calls the report writer for each system in a list of the Services Systems
	 * @param systemName	String containing the system name to produce a fact sheet
	 */
	@Override
	public void generateSystemReport(String systemName, boolean showMessage) {
		boolean dbcheck = retrieveDatabases();
		if (!dbcheck) {
			return;
		}
		
		lpiSystemList = createSystemList(lpiSystemsQuery);
		lpniSystemList = createSystemList(lpniSystemsQuery);
		highSystemList = createSystemList(highSystemsQuery);
		
		Hashtable queryResults = processQueries(systemName);
		ArrayList serviceResults = (ArrayList) queryResults.get(ConstantsTAP.PPI_QUERY);
		String service = null;
		for (int i = 0; i < serviceResults.size(); i++) {	
			ArrayList ppi = (ArrayList) serviceResults.get(i);
			for(int j = 0; j < ppi.size(); j++) {
				service = (String) ppi.get(j);
			}
		}
		writeToFile(service, systemName, queryResults);
		if (showMessage) {
			Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\FactSheets\\");
		}
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the DHMSM Disposition fact sheet reports
	 * @param systemName	String containing the system name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	@Override
	public Hashtable processQueries(String systemName) {
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		systemDescriptionQuery = systemDescriptionQuery.replaceAll("AHLTA", systemName);
		ppiQuery = ppiQuery.replaceAll("ASIMS", systemName);
		systemPOCQuery = systemPOCQuery.replaceAll("AHLTA", systemName);
		siteListQuery = siteListQuery.replaceAll("AHLTA", systemName);
		budgetQuery = budgetQuery.replaceAll("APEQS", systemName);
		sysHighlightsQuery = sysHighlightsQuery.replaceAll("ASIMS", systemName);
		interfacesQuery = interfacesQuery.replaceAll("ASIMS", systemName);
		referenceRepositoryQuery = referenceRepositoryQuery.replaceAll("ABACUS", systemName);
		rtmQuery = rtmQuery.replaceAll("AHLTA", systemName);
		
		
		ArrayList<ArrayList<Object>> siteResultsList = runQuery(tapSiteEngine, siteListQuery);
		ArrayList<ArrayList<Object>> budgetResultsList = runQuery(tapCostEngine, budgetQuery);		
		ArrayList<String> systemDescriptionResultsList = runQuery(tapCoreEngine, systemDescriptionQuery);
		ArrayList<String> systemHighlightsList = runQuery(tapCoreEngine, sysHighlightsQuery);
		ArrayList<String> pocList = runQuery(tapCoreEngine, systemPOCQuery);		
		ArrayList<String> ppiList = runQuery(tapCoreEngine, ppiQuery);
		ArrayList<ArrayList<Object>> interfacesResultsList = runQuery(tapCoreEngine, interfacesQuery);
		ArrayList<String> referenceRepositoryList = runQuery(tapCoreEngine, referenceRepositoryQuery);
		ArrayList<String> rtmList = runQuery(tapCoreEngine, rtmQuery);

		//ArrayList<Integer> capabilitiesSupportedResultsList = dhelp.getNumOfCapabilitiesSupported(systemName);
		
		calculateTransitionPercentageValues(systemName);
				
		returnHash.put(ConstantsTAP.SITE_LIST_QUERY, siteResultsList);
		returnHash.put(ConstantsTAP.BUDGET_QUERY, budgetResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_DESCRIPTION_QUERY, systemDescriptionResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY, systemHighlightsList);
		returnHash.put(ConstantsTAP.POC_QUERY, pocList);		
		returnHash.put(ConstantsTAP.PPI_QUERY, ppiList);
		returnHash.put(ConstantsTAP.LIST_OF_INTERFACES_QUERY, interfacesResultsList);
		//returnHash.put(ConstantsTAP.CAPABILITIES_SUPPORTED_QUERY, capabilitiesSupportedResultsList);
		returnHash.put(ConstantsTAP.LPI_SYSTEMS_QUERY, lpiSystemList);
		returnHash.put(ConstantsTAP.LPNI_SYSTEMS_QUERY, lpniSystemList);
		returnHash.put(ConstantsTAP.HIGH_SYSTEMS_QUERY, highSystemList);
		returnHash.put(ConstantsTAP.REFERENCE_REPOSITORY_QUERY, referenceRepositoryList);
		returnHash.put(ConstantsTAP.RTM_QUERY, rtmList);
		returnHash.put(ConstantsTAP.DHMSM_DATA_PROVIDED_PERCENT, percentDataCreatedDHMSMProvide);
		returnHash.put(ConstantsTAP.DHMSM_BLU_PROVIDED_PERCENT, percentBLUCreatedDHMSMProvide);
		
		return returnHash;	
	}
	
	/**
	 * Create the report file name and location, and call the writer to write the report for the specified system
	 * Create the location for the fact sheet report template
	 * @param service			String containing the service category of the system
	 * @param systemName		String containing the system name to produce the fact sheet
	 * @param systemDataHash 	Hashtable containing the results for the query for the specified system
	 */
	public void writeToFile(String service, String systemName, Hashtable systemDataHash) {
		DHMSMDispositionFactSheetWriter writer = new DHMSMDispositionFactSheetWriter();
		String folder = DIR_SEPARATOR + "export" + DIR_SEPARATOR + 
					"Reports" + DIR_SEPARATOR + "FactSheets" + DIR_SEPARATOR;
		String writeFileName;

		writeFileName = systemName.replaceAll(":", "") + "_Disposition_Fact_Sheet_" + 
				DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";
		
		String fileLoc = workingDir + folder + writeFileName;
		String templateFileName = "System_Disposition_Fact_Sheet_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;
		logger.info(fileLoc);
		
		writer.createWorkbook(systemName, templateLoc);
		//For the DHSMSM Transition Report Interface tabs
		boolean systemProbabilityHigh = false;
		if (highSystemList.contains(systemName))
			systemProbabilityHigh = true;
		
		String systemQueryURI = "<" + sysBaseURI + systemName + ">"; 
		proposedFutureICDQuery = proposedFutureICDQuery.replace("@SYSTEM@", systemQueryURI);
		decommissionedFutureICDQuery = decommissionedFutureICDQuery.replace("@SYSTEM@", systemQueryURI);
		allPresentICDQuery = allPresentICDQuery.replace("@SYSTEM@", systemQueryURI);
		
		HashMap<String, Object> sysProposedFutureICD = getQueryDataWithHeaders(FutureDB, proposedFutureICDQuery);
		HashMap<String, Object> sysDecommissionedFutureICD = getQueryDataWithHeaders(FutureDB, decommissionedFutureICDQuery);
		HashMap<String, Object> sysPersistentICD = determinePersistentICDs(sysDecommissionedFutureICD);
		
		ArrayList<Object[]> devICDList = (ArrayList<Object[]>)sysProposedFutureICD.get("data");
		ArrayList<Object[]> decomICDList = (ArrayList<Object[]>)sysDecommissionedFutureICD.get("data");
		ArrayList<Object[]> sustainICDList = (ArrayList<Object[]>)sysPersistentICD.get("data");
		
		if (!systemProbabilityHigh) {			
			if (devICDList.size() == 0)
				writer.hideWorkSheet("Future Interface Development");
			else {
				writer.writeListSheet("Future Interface Development", sysProposedFutureICD, false);
			}	
			if (decomICDList.size() == 0)
				writer.hideWorkSheet("Future Interface Decommission");
			else {
				writer.writeListSheet("Future Interface Decommission", sysDecommissionedFutureICD, false);
			}	
			if (sustainICDList.size() == 0)
				writer.hideWorkSheet("Future Interface Sustainment");
			else {
				writer.writeListSheet("Future Interface Sustainment", sysPersistentICD, false);
			}
		} else {
			decomICDList.addAll(sustainICDList);
			sysDecommissionedFutureICD.put("data", decomICDList);
			sustainICDList = new ArrayList<Object[]>();
			devICDList = new ArrayList<Object[]>();
			writer.hideWorkSheet("Future Interface Development");
			writer.hideWorkSheet("Future Interface Sustainment");
			writer.writeListSheet("Future Interface Decommission", sysDecommissionedFutureICD, systemProbabilityHigh);
		}
		
		
		//DHMSM Integration Transition Cost writing
		DHMSMIntegrationTransitionCostWriter costWriter = null;
		String systemURI = sysBaseURI + systemName; 
		try {
			costWriter = new DHMSMIntegrationTransitionCostWriter();
			costWriter.setSysURI(systemURI);
			writer.writeTransitionCosts(costWriter, systemProbabilityHigh);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			Utility.showError(e.getMessage());
		}
		
		writer.exportFactSheets(fileLoc, systemDataHash, devICDList.size(), decomICDList.size(), sustainICDList.size());
	}
	
	private HashMap<String, Object> getQueryDataWithHeaders(IDatabaseEngine engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(DHMSMTransitionUtility.HEADER_KEY, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++)
			{
				Object dataElem = sjss.getVar(names[i]);
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
				{
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				}
				dataRow[i] = dataElem;
			}
			dataToAddArr.add(dataRow);
		}

		dataHash.put(DHMSMTransitionUtility.DATA_KEY, dataToAddArr);

		return dataHash;
	}
	
	private HashMap<String, Object> determinePersistentICDs(HashMap<String, Object> decommissionedFutureICDs) {
		HashMap<String, Object> retHash = new HashMap<String, Object>();
		
		HashMap<String, Object> allICDs = getQueryDataWithHeaders(TAP_Core_Data, allPresentICDQuery);
		ArrayList<Object[]> allData = (ArrayList<Object[]>) allICDs.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> removedData = (ArrayList<Object[]>) decommissionedFutureICDs.get(DHMSMTransitionUtility.DATA_KEY);
		
		Iterator<Object[]> itOverArray = allData.iterator();
		while(itOverArray.hasNext()) {
			Object[] icdRow = itOverArray.next();
			INNER: for(Object[] removeICD : removedData) {
				if(icdRow[2].toString().equals(removeICD[2].toString())) {
					itOverArray.remove();
					break INNER;
				}
			}
		}
		
		retHash.put(DHMSMTransitionUtility.HEADER_KEY, allICDs.get(DHMSMTransitionUtility.HEADER_KEY));
		retHash.put(DHMSMTransitionUtility.DATA_KEY, allData);
		
		return retHash;
	}
	
	public void calculateTransitionPercentageValues(String systemName) {
		sysNumBLUQuery = sysNumBLUQuery.replaceAll("ABACUS", systemName);
		sysAndDHMSMNumBLUQuery = sysAndDHMSMNumBLUQuery.replaceAll("ABACUS", systemName);
		
		ArrayList sysNumBLU = runQuery(tapCoreEngine, sysNumBLUQuery);
		ArrayList sysAndDHMSMBLU = runQuery(tapCoreEngine, sysAndDHMSMNumBLUQuery);
		
		DHMSMHelper dhelp = new DHMSMHelper();
		dhelp.runData(TAP_Core_Data);
		
		ArrayList<String> dataSystemSORList = dhelp.getAllDataFromSys(systemName, "C");
		int numOfDataSystemSOR = dataSystemSORList.size();
		ArrayList<String> dataSystemSORAndDHMSMCreateList = dhelp.getDataObjectListSupportedFromSystem(systemName, "C", "C");
		int numOfDataSystemSORAndDHMSMCreate = dataSystemSORAndDHMSMCreateList.size();
				
		double numOfBLUSystemSOR = 0;
		double numOfBLUSystemSORAndDHMSMCreate = 0;

		if (!((ArrayList<Object>) sysNumBLU).isEmpty()) { 
			numOfBLUSystemSOR = (double) ((ArrayList<Object>) sysNumBLU.get(0)).get(0);
		}
		if (!((ArrayList<Object>) sysAndDHMSMBLU).isEmpty()) { 
			numOfBLUSystemSORAndDHMSMCreate = (double) ((ArrayList<Object>) sysAndDHMSMBLU.get(0)).get(0);
		}
		
		//Calculate % of BLU that DHMSM will provide for the given system
		if (numOfBLUSystemSOR == 0)
			percentBLUCreatedDHMSMProvide = 0;
		else if (numOfBLUSystemSORAndDHMSMCreate == 0)
			percentBLUCreatedDHMSMProvide = 0;
		else if (numOfBLUSystemSOR == numOfBLUSystemSORAndDHMSMCreate)
			percentBLUCreatedDHMSMProvide = 1.0;
		else {
			double bluPercent = (double) numOfBLUSystemSORAndDHMSMCreate / (double) numOfBLUSystemSOR;
			percentBLUCreatedDHMSMProvide = (bluPercent);
		}
		
		//Calculate % of Data Objects that DHMSM will provide for the given system
		if(numOfDataSystemSOR == 0)
			percentDataCreatedDHMSMProvide = 0;
		else if((numOfDataSystemSOR == numOfDataSystemSORAndDHMSMCreate))
			percentDataCreatedDHMSMProvide = 1.0;
		else if(numOfDataSystemSORAndDHMSMCreate == 0)
			percentDataCreatedDHMSMProvide = 0;
		else {
			double dataPercent = (double)numOfDataSystemSORAndDHMSMCreate/ (double)numOfDataSystemSOR;
			percentDataCreatedDHMSMProvide = (dataPercent);
		}
	}

}
