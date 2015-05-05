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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.FactSheetSysSimCalculator;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the Services Fact Sheet Reports
 * Used in conjunction with FactSheetListener
 */
public class FactSheetProcessor {
	private static final Logger logger = LogManager.getLogger(FactSheetProcessor.class.getName());
	protected String tapCoreEngine = "TAP_Core_Data";
	protected String tapSiteEngine = "TAP_Site_Data";
	protected String tapCostEngine = "TAP_Cost_Data";
	protected String hrCoreEngine = "HR_Core";
	protected String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	protected FactSheetSysSimCalculator sysSim;
	protected DHMSMHelper dhelp;
	
	//Report Queries
	protected String systemDescriptionQuery = "SELECT DISTINCT ?Description WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description ;} }";
	protected String ppiQuery = "SELECT DISTINCT ?SystemOwner WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>} {?System ?OwnedBy ?SystemOwner} }";
	protected String systemPOCQuery = "SELECT DISTINCT ?POC WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/POC> ?POC ;} }";
	protected String siteListQuery = "SELECT DISTINCT ?DCSite (COALESCE(?DCSiteCmdCode,\"\") AS ?SiteCmdCode) WHERE { SELECT DISTINCT ?DCSite (GROUP_CONCAT(?CmdCode1 ; SEPARATOR = ', ') AS ?DCSiteCmdCode) WHERE {SELECT DISTINCT ?DCSite (SUBSTR(STR(?CmdCode),52) AS ?CmdCode1) { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?CmdCode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSiteCmdCode>;} BIND (<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) MINUS {BIND(<http://health.mil/ontologies/Concept/DCSiteCmdCode/UNK> AS ?CmdCode)} {?SystemDCSite ?DeployedAt ?DCSite;} {?System ?DeployedAt1 ?SystemDCSite;}{?DCSite ?Has ?CmdCode;} } } GROUP BY ?DCSite }";
	protected String budgetQuery = "SELECT DISTINCT ?GLTag (max(coalesce(?FY14,0)) as ?fy14) (max(coalesce(?FY15,0)) as ?fy15) (max(coalesce(?FY16,0)) as ?fy16) (max(coalesce(?FY17,0)) as ?fy17) (max(coalesce(?FY18,0)) as ?fy18) (max(coalesce(?FY19,0)) as ?fy19) WHERE { BIND(<http://health.mil/ontologies/Concept/System/APEQS> AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;} {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FundedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/FundedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem} {?SystemBudgetGLItem ?TaggedBy ?GLTag} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY14>, ?Budget,0) as ?FY14) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY16>, ?Budget,0) as ?FY16) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY17>, ?Budget,0) as ?FY17) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY18>, ?Budget,0) as ?FY18) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY19>, ?Budget,0) as ?FY19) } GROUP BY ?System ?GLTag";
	protected String sysHighlightsQuery = "SELECT DISTINCT ?NumberOfUsers ?UserConsoles ?RequiredAvailability ?ActualAvailability ?Transactional ?DailyTransactions ?DateATO ?EndOfSupport ?GarrisonTheater WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NumberOfUsers} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/User_Consoles> ?UserConsoles} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Required> ?RequiredAvailability} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?ActualAvailability} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?DailyTransactions} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?DateATO} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?EndOfSupport} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GarrisonTheater} } OPTIONAL{ BIND(IF(?Trans = \"Yes\", \"Transactional\",\"Intelligence\") AS ?Transactional) } }";
	protected String interfacesQuery = "SELECT DISTINCT ?type ?Interface ?UpstreamSystem ?DownstreamSystem ?Data ?SOATransition ?ReplacementSOAService ?Protocol (COALESCE(?UpstreamFur, \"none\") AS ?UpstreamFurther) WHERE { { SELECT DISTINCT ?System ?type ?Interface ?Data ?Format ?Frequency ?ReplacementSOAService ?Protocol (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?UpstreamFur WHERE { BIND(<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{{?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>} {?ReplacementSOAService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ReplacementSOAService ?exposes ?Data}} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt>} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?Interface ?Has ?Protocol} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System ?Upstream ?Interface ;} {?Interface ?Downstream ?DownstreamSys ;} BIND(\"Downstream\" AS ?type) OPTIONAL{{?InterfaceFurther <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carriesFurther <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?InterfaceFurther ?carriesFurther ?Data;} {?DownstreamFurther <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?InterfaceFurther ?DownstreamFurther ?System ;}}BIND(IF(BOUND(?InterfaceFurther),\"exists\",\"none\") AS ?UpstreamFur)} } UNION { SELECT DISTINCT ?System ?type ?Interface ?Data ?Format ?Frequency ?ReplacementSOAService ?Protocol (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?UpstreamFur WHERE { BIND(<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{{?exposes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>} {?ReplacementSOAService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ReplacementSOAService ?exposes ?Data}} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt>} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?Interface ?Has ?Protocol} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?System ;} BIND(\"Upstream\" AS ?type) OPTIONAL{{?InterfaceFurther <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carriesFurther <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?InterfaceFurther ?carriesFurther ?Data;} {?DownstreamFurther <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?InterfaceFurther ?DownstreamFurther ?UpstreamSys ;}}BIND(IF(BOUND(?InterfaceFurther),\"exists\",\"none\") AS ?UpstreamFur)} }}";
	
	public void setDHMSMHelper(DHMSMHelper dhelp)
	{
		this.dhelp = dhelp;
	}

	/**
	 * Runs a query on a specific database and returns the result as an ArrayList
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list 		ArrayList<ArrayList<Object>> containing the results of the query 
	 */
	public ArrayList runQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		*/
		
		String[] names = wrapper.getVariables();
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		while (wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();
			ArrayList<Object> values = new ArrayList<Object>();
			for (int colIndex = 0; colIndex < names.length; colIndex++) {
				if (sjss.getVar(names[colIndex]) != null) {
					if (sjss.getVar(names[colIndex]) instanceof Double) {
						values.add(colIndex, (Double) sjss.getVar(names[colIndex]));
					}
					else values.add(colIndex, (String) sjss.getVar(names[colIndex]));						
				}
			}
			list.add(values);
		}

		return list;
	}

	/**
	 * Runs a life cycle query on a specific database and return a result set as an ArrayList 
	 * Processes the life cycle query results before populating the result set
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list			ArrayList<String> containing the results of the query 
	 */
	public ArrayList runLifeCycleQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
*/
		String[] names = wrapper.getVariables();
		ArrayList<String> list = new ArrayList<String>();
		Calendar now = Calendar.getInstance();
		int currYear=now.get(Calendar.YEAR);
		int currMonth=now.get(Calendar.MONTH);

		while (wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();

			if (sjss.getVar(names[1]) != null) {
				String date = ((String) sjss.getVar(names[1])).replaceAll("\"", "");
				if (!date.equals("TBD")) {	
					String lifeCycleType;
					try{
						int year=Integer.parseInt(date.substring(0,4));
						int month=Integer.parseInt(date.substring(5,7));												

						if((year<currYear)||(year==currYear && month<=currMonth+6)||(year==currYear+1&&month<=currMonth+6-12))
							lifeCycleType="Retired_(Not_Supported)";
						else if(year<=currYear||(year==currYear+1&&month<=currMonth))
							lifeCycleType="Sunset_(End_of_Life)";
						else if(year<=currYear+2||(year==currYear+3&&month<=currMonth))
							lifeCycleType="Supported";
						else
							lifeCycleType="GA_(Generally_Available)";
					}catch(NumberFormatException e)
					{
						lifeCycleType = "TBD";
					}

					list.add(lifeCycleType);								
				}
				else list.add(date);						
			}

		}

		return list;
	}

	/**
	 * Creates a list of all the Services systems
	 * @param query 		String containing the query to return the list of all Services systems
	 * @return list			ArrayList<String> containing the list of all the Services systems for which fact sheet reports need to be created
	 */
	public ArrayList<String> createSystemList(String query) {
		ArrayList<String> list = new ArrayList<String>();
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine + "");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
*/
		String[] names = wrapper.getVariables();
		while (wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();				
			String sys = (String)sjss.getVar(names[0]);
			list.add(sys);
		}
		return list;
	}

	/**
	 * Processes and stores the fact sheet queries and calls the report writer for each system in a list of the Services Systems
	 */
	public void generateReports() {
		sysSim = new FactSheetSysSimCalculator();		

		ArrayList<String> systemList = createSystemList("SELECT DISTINCT ?System WHERE{{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy> ;}{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;}{?System ?OwnedBy ?SystemOwner}}ORDER BY ?System BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}");

		//	ArrayList<String> systemList = createSystemList("SELECT DISTINCT ?System WHERE{{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy> ;}{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;}{?System ?OwnedBy ?SystemOwner}}ORDER BY ?System BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}");

		boolean shouldStart = true;
		for (int i=0; i<systemList.size(); i++) {
			String systemName = systemList.get(i);
			//to start at a specific system,
			//set shouldStart equal to false before the forloop
			//then set systemName = to the one to start at 
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
	public void generateSystemReport(String systemName, boolean showMessage) {
		sysSim = new FactSheetSysSimCalculator();		

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
	 * Contains all the queries required to return the necessary data for the fact sheet reports
	 * @param systemName	String containing the system name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processQueries(String systemName) {
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	
	
		systemDescriptionQuery = systemDescriptionQuery.replaceAll("AHLTA", systemName);
		ppiQuery = ppiQuery.replaceAll("ASIMS", systemName);
		systemPOCQuery = systemPOCQuery.replaceAll("AHLTA", systemName);
		siteListQuery = siteListQuery.replaceAll("AHLTA", systemName);
		budgetQuery = budgetQuery.replaceAll("APEQS", systemName);
		sysHighlightsQuery = sysHighlightsQuery.replaceAll("ASIMS", systemName);
		interfacesQuery = interfacesQuery.replaceAll("ASIMS", systemName);

		//System Maturity Query
		String systemMaturityQuery = "SELECT DISTINCT (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) WHERE { BIND(<http://health.mil/ontologies/Concept/System/APEQS> AS ?System) OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv;} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm .} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm .} } }";
		systemMaturityQuery = systemMaturityQuery.replaceAll("APEQS", systemName);

		//System Software Query
		String systemSWQuery = "SELECT DISTINCT ?SoftwareVersion (COALESCE(?EOL,\"TBD\") AS ?eol) WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>;}{?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>;}{?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>;}  {?System ?Has ?SoftwareModule} {?SoftwareModule ?TypeOf ?SoftwareVersion } OPTIONAL{?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?EOL} }";
		systemSWQuery = systemSWQuery.replaceAll("AHLTA", systemName);

		//System Hardware Query
		String systemHWQuery = "SELECT DISTINCT ?HardwareVersion (COALESCE(?EOL,\"TBD\") AS ?eol) WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>;}{?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>;}{?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>;}  {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion } OPTIONAL{?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?EOL} }";
		systemHWQuery = systemHWQuery.replaceAll("AHLTA", systemName);		

		//Data Provenance Query
		String dataProvenanceQuery = "SELECT DISTINCT ?crm ?data (COALESCE( (COUNT(DISTINCT ?downStreamSystem)),0 ) AS ?count) WHERE { BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?system) {?providesData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system ?providesData ?data} {?providesData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}  {?system ?providesICD ?icd} OPTIONAL { {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?icd ?payload ?data} {?consume <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>} {?downStreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?icd ?consume ?downStreamSystem} {?providesICD <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} }} GROUP BY ?data ?crm ?system ORDER BY ?crm";
		dataProvenanceQuery = dataProvenanceQuery.replaceAll("AHLTA", systemName);

		//Business Logic Query
		String businessLogicQuery = "SELECT DISTINCT ?blu WHERE {BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System) {?provides <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?System ?provides ?blu} }";		
		businessLogicQuery = businessLogicQuery.replaceAll("AHLTA", systemName);

		//Unique Data Objects Query
		String uniqueDataQuery = "SELECT (SAMPLE(?system) AS ?sys1) (SAMPLE(?data) AS ?DATA) (COALESCE(COUNT(DISTINCT(?system)), 0) AS ?totalSys) WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system ?provide ?data} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} FILTER (?crm = \"C\") } GROUP BY ?data ORDER BY ?totalSys";

		//Unique BLUs Query
		String uniqueBLUQuery = "SELECT (SAMPLE(?system) AS ?sys1) (SAMPLE(?blu) AS ?BLU) (COALESCE(COUNT(DISTINCT(?system)), 0) AS ?totalSys) WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?system ?provide ?blu ;} } GROUP BY ?blu ORDER BY ?totalSys";

		//Types of Users Query
		String userTypesQuery = "SELECT DISTINCT ?Personnel WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>}{?Personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel>;} {?System ?UsedBy ?Personnel} }";
		userTypesQuery = userTypesQuery.replaceAll("ASIMS", systemName);

		//User Interface Query
		String userInterfaceQuery = "SELECT DISTINCT ?UserInterface WHERE { BIND (<http://health.mil/ontologies/Concept/System/ASIMS> AS ?System) {?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>} {?System ?Utilizes ?UserInterface} }";
		userInterfaceQuery = userInterfaceQuery.replaceAll("ASIMS", systemName);

		ArrayList<String> systemSWResultsList = runLifeCycleQuery(tapCoreEngine, systemSWQuery);
		ArrayList<String> systemHWResultsList = runLifeCycleQuery(tapCoreEngine, systemHWQuery);
		ArrayList<Double> systemMaturityResultsList = runQuery(tapCoreEngine, systemMaturityQuery);
		ArrayList<ArrayList<Object>> dataProvenanceResultsList = runQuery(tapCoreEngine, dataProvenanceQuery);
		ArrayList<ArrayList<Object>> businessLogicResultsList = runQuery(tapCoreEngine, businessLogicQuery);
		ArrayList<ArrayList<Object>> sysSimList = sysSim.priorityAllDataHash.get(systemName);
		ArrayList<String> sysList = sysSim.prioritySysHash.get(systemName);
		ArrayList<Double> valueList = sysSim.priorityValueHash.get(systemName);
		ArrayList<ArrayList<Object>> uniqueDataResultsList = runQuery(tapCoreEngine, uniqueDataQuery);
		ArrayList<ArrayList<Object>> uniqueBLUResultsList = runQuery(tapCoreEngine, uniqueBLUQuery);
		ArrayList<ArrayList<String>> userTypesList = runQuery(tapCoreEngine, userTypesQuery);
		ArrayList<ArrayList<String>> userInterfacesList = runQuery(tapCoreEngine, userInterfaceQuery);
		
		ArrayList<ArrayList<Object>> siteResultsList = runQuery(tapSiteEngine, siteListQuery);
		ArrayList<ArrayList<Object>> budgetResultsList = runQuery(tapCostEngine, budgetQuery);		
		ArrayList<String> systemDescriptionResultsList = runQuery(tapCoreEngine, systemDescriptionQuery);
		ArrayList<String> systemHighlightsList = runQuery(tapCoreEngine, sysHighlightsQuery);
		ArrayList<String> pocList = runQuery(tapCoreEngine, systemPOCQuery);		
		ArrayList<String> ppiList = runQuery(tapCoreEngine, ppiQuery);
		ArrayList<ArrayList<Object>> interfacesResultsList = runQuery(tapCoreEngine, interfacesQuery);

		ArrayList<Integer> capabilitiesSupportedResultsList = dhelp.getNumOfCapabilitiesSupported(systemName);
		

		returnHash.put(ConstantsTAP.SYSTEM_SW_QUERY, systemSWResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_HW_QUERY, systemHWResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_MATURITY_QUERY, systemMaturityResultsList);
		returnHash.put(ConstantsTAP.DATA_PROVENANCE_QUERY, dataProvenanceResultsList);
		returnHash.put(ConstantsTAP.BUSINESS_LOGIC_QUERY, businessLogicResultsList);
		if (sysSimList != null) {
			returnHash.put(ConstantsTAP.SYS_SIM_QUERY, sysSimList);
			returnHash.put(ConstantsTAP.SYS_QUERY, sysList);
			returnHash.put(ConstantsTAP.VALUE_QUERY, valueList);
		}
		returnHash.put(ConstantsTAP.UNIQUE_DATA_PROVENANCE_QUERY, uniqueDataResultsList);
		returnHash.put(ConstantsTAP.UNIQUE_BUSINESS_LOGIC_QUERY, uniqueBLUResultsList);
		returnHash.put(ConstantsTAP.USER_TYPES_QUERY, userTypesList);
		returnHash.put(ConstantsTAP.USER_INTERFACES_QUERY, userInterfacesList);		
		returnHash.put(ConstantsTAP.SITE_LIST_QUERY, siteResultsList);
		returnHash.put(ConstantsTAP.BUDGET_QUERY, budgetResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_DESCRIPTION_QUERY, systemDescriptionResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_HIGHLIGHTS_QUERY, systemHighlightsList);
		returnHash.put(ConstantsTAP.POC_QUERY, pocList);		
		returnHash.put(ConstantsTAP.PPI_QUERY, ppiList);
		returnHash.put(ConstantsTAP.LIST_OF_INTERFACES_QUERY, interfacesResultsList);
		returnHash.put(ConstantsTAP.CAPABILITIES_SUPPORTED_QUERY,capabilitiesSupportedResultsList);

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
		FactSheetWriter writer = new FactSheetWriter();
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator") + "FactSheets" + System.getProperty("file.separator");
		String writeFileName;
		//		if (service != null) {
		//			writeFileName = service.replaceAll(" ", "_") + "_" + systemName.replaceAll(":", "") + "_Fact_Sheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";
		//		}
		//		else{ 
		writeFileName = systemName.replaceAll(":", "") + "_Fact_Sheet_" + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";
		//		}

		String fileLoc = workingDir + folder + writeFileName;
		String templateFileName = "Fact_Sheet_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;
		logger.info(fileLoc);	

		writer.exportFactSheets(systemName, fileLoc, templateLoc, systemDataHash);
	}
}