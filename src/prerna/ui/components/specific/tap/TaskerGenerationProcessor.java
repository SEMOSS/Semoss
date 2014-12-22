/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.io.File;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.TaskerGenerationWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the Tasker for a specific system
 * Used in conjunction with TaskerGenerationListener
 */
public class TaskerGenerationProcessor {
	static final Logger logger = LogManager.getLogger(TaskerGenerationProcessor.class.getName());
	public String coreDB = "";
	String tapSiteEngine = "TAP_Site_Data";
//	String tapPortfolioEngine = "TAP_Portfolio";
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

	/**
	 * Runs a query on a specific database and returns the result as an ArrayList
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list 		ArrayList<ArrayList<Object>> containing the results of the query 
	 */
	public ArrayList runQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				ArrayList<Object> values = new ArrayList<Object>();
				for (int colIndex = 0; colIndex < names.length; colIndex++) {
					if (sjss.getVar(names[colIndex]) != null) {
						if (sjss.getVar(names[colIndex]) instanceof Double) {
							values.add(colIndex, (Double) sjss.getVar(names[colIndex]));
						}
						else if(((String)sjss.getVar(names[colIndex])).contains("T00:00:00.000Z"))
						{
							String dateVal = (String) sjss.getVar(names[colIndex]);
							dateVal = dateVal.substring(0,dateVal.indexOf("T"));
							values.add(colIndex, dateVal);
						}
						else values.add(colIndex, (String) sjss.getVar(names[colIndex]));						
					}
				}
				list.add(values);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}

		return list;
	}


	/**
	 * Processes and stores the tasker queries and calls the report writer to output the tasker
	 * @param systemName	String containing the system name to produce the tasker for
	 */
	public void generateSystemTasker(String systemName) {
		Hashtable queryResults = processQueries(systemName);
		writeToFile(systemName, queryResults);
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\");
	}

	/**
	 * Contains all the queries required to return the necessary data for the fact sheet reports
	 * @param systemName	String containing the system name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processQueries(String systemName) {
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		//System Name Query
		String sysNameQuery = "SELECT DISTINCT ?System ?FullSystemName WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/Full_System_Name> ?FullSystemName ;}}}";
		sysNameQuery = sysNameQuery.replaceAll("ASIMS", systemName);
		
		//System Highlights Query
		String sysHighlightsQuery = "SELECT DISTINCT ?Description ?NumberOfUsers ?UserConsoles ?RequiredAvailability ?ActualAvailability ?DailyTransactions ?DateATO ?GarrisonTheater ?EndOfSupport WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description ;}}OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NumberOfUsers} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/User_Consoles> ?UserConsoles} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Required> ?RequiredAvailability} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?ActualAvailability} }OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?DailyTransactions} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?DateATO} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?EndOfSupport} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GarrisonTheater} } OPTIONAL{ BIND(IF(?Trans = \"Yes\", \"Transactional\",\"Intelligence\") AS ?Transactional) } }";
		sysHighlightsQuery = sysHighlightsQuery.replaceAll("ASIMS", systemName);

		//Types of Users Query
		String userTypesQuery = "SELECT DISTINCT ?Users WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>}{?Users <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel>} {?System ?UsedBy ?Users} }";
		userTypesQuery = userTypesQuery.replaceAll("ASIMS", systemName);

		//User Interface Query
		String userInterfaceQuery = "SELECT DISTINCT ?UserInterface WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>} {?System ?Utilizes ?UserInterface} }";
		userInterfaceQuery = userInterfaceQuery.replaceAll("ASIMS", systemName);
		

		//Business Processes Supported Query
		String businessProcessQuery = "SELECT DISTINCT ?System ?BusinessProcess WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?System ?Supports ?BusinessProcess}} ORDER BY ?BusinessProcess";
		businessProcessQuery = businessProcessQuery.replaceAll("ASIMS", systemName);

		//Activities Supported Query
		String activityQuery = "SELECT DISTINCT ?System ?Activity WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?System ?Supports ?Activity}} ORDER BY ?Activity";
		activityQuery = activityQuery.replaceAll("ASIMS", systemName);
		
		//BLU Supported Query
		String bluQuery = "SELECT DISTINCT ?System ?BusinessLogicUnit WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?System ?Provide ?BusinessLogicUnit}} ORDER BY ?BusinessLogicUnit";
		bluQuery = bluQuery.replaceAll("ASIMS", systemName);
		
		//Data Supported Query
		String dataQuery = "SELECT DISTINCT ?System ?DataObject ?CRM WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?System ?Provide ?DataObject}{?Provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM}} ORDER BY ?DataObject";
		dataQuery = dataQuery.replaceAll("ASIMS", systemName);
		
		//System Owner Query
		String ownerQuery ="SELECT DISTINCT ?Owner WHERE {BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?Owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?Owner} }";
		ownerQuery = ownerQuery.replaceAll("ASIMS", systemName);

		//List of Interfaces Query		
		String interfacesQuery = "SELECT DISTINCT ?Interface (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Data ?Frequency ?Protocol ?Format ?Comments ?Source WHERE { { SELECT DISTINCT ?System ?Interface ?Data ?Frequency ?Protocol ?Format ?Source (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) WHERE { BIND(<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System){?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Source> ?Source ;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System ?Upstream ?Interface ;} {?Interface ?Downstream ?DownstreamSys ;} } } UNION { SELECT DISTINCT ?System ?Interface ?Data ?Frequency ?Protocol ?Format ?Source (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) WHERE { BIND(<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System){?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Source> ?Source ;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?System ;}} }} ORDER BY ?Interface";
		interfacesQuery = interfacesQuery.replaceAll("ASIMS", systemName);
		
		//Budget Query
//		String budgetQuery = "SELECT DISTINCT ?GLTag (max(coalesce(?FY14,0)) as ?fy14) (max(coalesce(?FY15,0)) as ?fy15) (max(coalesce(?FY16,0)) as ?fy16) (max(coalesce(?FY17,0)) as ?fy17) (max(coalesce(?FY18,0)) as ?fy18) (max(coalesce(?FY19,0)) as ?fy19) WHERE { BIND(<http://health.mil/ontologies/Concept/System/APEQS> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;} {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem} {?SystemBudgetGLItem ?TaggedBy ?GLTag} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY14>, ?Budget,0) as ?FY14) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY16>, ?Budget,0) as ?FY16) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY17>, ?Budget,0) as ?FY17) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY18>, ?Budget,0) as ?FY18) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY19>, ?Budget,0) as ?FY19) } GROUP BY ?System ?GLTag";		
//		budgetQuery = budgetQuery.replaceAll("APEQS", systemName);

		//Site List Query
		String siteListQuery = "SELECT DISTINCT ?SiteName WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?SiteName <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} BIND (<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?SystemDCSite ?DeployedAt ?SiteName;}{?System ?DeployedAt1 ?SystemDCSite;} }";		
		siteListQuery = siteListQuery.replaceAll("AHLTA", systemName);		

		
		//System Software Query
		String systemSWQuery = "SELECT DISTINCT ?SoftwareVersion ?Software ?Comments (COALESCE(?Units,0.0) AS ?units) ?EOL ?Sub WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>;}{?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>;}{?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>;}  {?System ?Consists ?SoftwareModule} {?SoftwareModule ?TypeOf ?SoftwareVersion } OPTIONAL{?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Comments> ?Comments} OPTIONAL{?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Units} OPTIONAL{?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?EOL}} ORDER BY ?SoftwareVersion";
		systemSWQuery = systemSWQuery.replaceAll("AHLTA", systemName);

		//System Hardware Query
		String systemHWQuery = "SELECT DISTINCT ?HardwareVersion ?manufacturer ?type ?model (COALESCE(?units,0.0) AS ?Units) ?lifecycle ?EOL ?SUB WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>;}{?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>;}{?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>;}  {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion }  OPTIONAL{{?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Manufacturer> ?manufacturer} {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Product_Type> ?type}{?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Model> ?model}}OPTIONAL{?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?units}OPTIONAL{{?Phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase>;}{?lifecycle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/LifeCycle>;}{?HardwareVersion ?Phase ?lifecycle}} OPTIONAL{?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?EOL} }ORDER BY ?HardwareVersion";
		systemHWQuery = systemHWQuery.replaceAll("AHLTA", systemName);

		//TError Query
		String terrorQuery = "SELECT DISTINCT ?TError (COALESCE(?source,\"TBD\") AS ?Source) ?percent WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?System ?Has ?TError} {?Has <http://semoss.org/ontologies/Relation/Contains/weight> ?percent ;}OPTIONAL{?TError <http://semoss.org/ontologies/Relation/Contains/Source> ?source ;}} ORDER BY ?TError";
		terrorQuery = terrorQuery.replaceAll("ASIMS", systemName);

		//sheet 1: System info
		ArrayList<String> systemNameList = runQuery(coreDB, sysNameQuery);
		ArrayList<String> systemHighlightsList = runQuery(coreDB, sysHighlightsQuery);
		ArrayList<ArrayList<String>> userTypesList = runQuery(coreDB, userTypesQuery);
		ArrayList<ArrayList<String>> userInterfacesList = runQuery(coreDB, userInterfaceQuery);

		ArrayList<ArrayList<String>> businessProcessList = runQuery(coreDB, businessProcessQuery);
		ArrayList<ArrayList<String>> activityList = runQuery(coreDB, activityQuery);
		ArrayList<ArrayList<String>> bluList = runQuery(coreDB, bluQuery);
		ArrayList<ArrayList<String>> dataList = runQuery(coreDB, dataQuery);
		
		ArrayList<ArrayList<Object>> interfacesResultsList = runQuery(coreDB, interfacesQuery);
		ArrayList<String> ownerList = runQuery(coreDB, ownerQuery);
//		ArrayList<ArrayList<Object>> budgetResultsList = runQuery(tapPortfolioEngine, budgetQuery);
		ArrayList<ArrayList<Object>> siteResultsList = runQuery(tapSiteEngine, siteListQuery);
		ArrayList<String> systemSWResultsList = runQuery(coreDB, systemSWQuery);
		ArrayList<String> systemHWResultsList = runQuery(coreDB, systemHWQuery);
		ArrayList<String> terrorResultsList = runQuery(coreDB, terrorQuery);
		
		//Sheet 1 System Info
		returnHash.put(ConstantsTAP.SYSTEM_NAME_QUERY, systemNameList);
		returnHash.put(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY, systemHighlightsList);
		returnHash.put(ConstantsTAP.USER_TYPES_QUERY, userTypesList);
		returnHash.put(ConstantsTAP.USER_INTERFACES_QUERY, userInterfacesList);
		returnHash.put(ConstantsTAP.BUSINESS_PROCESS_QUERY, businessProcessList);
		returnHash.put(ConstantsTAP.ACTIVITY_QUERY, activityList);
		returnHash.put(ConstantsTAP.BLU_QUERY, bluList);
		returnHash.put(ConstantsTAP.DATA_QUERY, dataList);
		returnHash.put(ConstantsTAP.LIST_OF_INTERFACES_QUERY, interfacesResultsList);
//		returnHash.put(ConstantsTAP.BUDGET_QUERY, budgetResultsList);
		returnHash.put(ConstantsTAP.SITE_LIST_QUERY, siteResultsList);
		returnHash.put(ConstantsTAP.PPI_QUERY, ownerList);
		returnHash.put(ConstantsTAP.SYSTEM_SW_QUERY, systemSWResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_HW_QUERY, systemHWResultsList);
		returnHash.put(ConstantsTAP.TERROR_QUERY, terrorResultsList);
		
		return returnHash;
	}

	/**
	 * Create the report file name and location, and call the writer to write the report for the specified system
	 * Create the location for the fact sheet report template
	 * @param service			String containing the service category of the system
	 * @param systemName		String containing the system name to produce the fact sheet
	 * @param systemDataHash 	Hashtable containing the results for the query for the specified system
	 */
	public void writeToFile(String systemName, Hashtable systemDataHash) {
		TaskerGenerationWriter writer = new TaskerGenerationWriter();
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		String writeFileName;

		writeFileName = "Tasker_for_" +systemName.replaceAll(":", "") + "_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		String templateFileName = "Tasker_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;
		logger.info(fileLoc);	

		writer.exportTasker(systemName, fileLoc, templateLoc, systemDataHash);
	}
}