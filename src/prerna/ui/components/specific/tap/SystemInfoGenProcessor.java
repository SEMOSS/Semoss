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
package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.Logger;

import prerna.poi.specific.SystemInfoGenWriter;
import prerna.poi.specific.TaskerGenerationWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the System Info Report
 * Used in conjunction with SystemInfoGenListener
 */
public class SystemInfoGenProcessor {
	Logger logger = Logger.getLogger(getClass());
	String tapCoreEngine = "TAP_Core_Data";
	String tapCostEngine = "TAP_Cost_Data";
	String tapSiteEngine = "TAP_Site_Data";
	String hrCoreEngine = "HR_Core";
	String workingDir = System.getProperty("user.dir");
	Hashtable<String,Hashtable> masterHash;
	ArrayList<String> sysList;
	ArrayList<String> capList;
	ArrayList<String> headersList;
	ArrayList<String> dataObjectList;
	String dataObjectBindings;
	/**
	 * Runs a query on a specific database and puts the results in the masterHash for a system
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		for(String head : names)
			if(!headersList.contains(head))
				headersList.add(head);
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				if(masterHash.containsKey(sys))
				{
					Hashtable sysHash = masterHash.get(sys);
					for (int colIndex = 1; colIndex < names.length; colIndex++) {
						String varName = names[colIndex];
						Object val = sjss.getVar(names[colIndex]);
						if (val != null) {
							if (val instanceof Double) {
								sysHash.put(varName, val);
							}
							else
							{
								if(sysHash.containsKey(varName))
									val = (String) sysHash.get(varName) +", " + (String) val;
								sysHash.put(varName, val);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runSystemListQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				Hashtable<String,Object> sysHash = new Hashtable<String,Object>();
				masterHash.put((String) sjss.getVar(names[0]),sysHash);
				sysList.add((String) sjss.getVar(names[0]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	/**
//	 * Runs a query on a specific engine to make a list of systems to report on
//	 * @param engineName 	String containing the name of the database engine to be queried
//	 * @param query 		String containing the SPARQL query to run
//	 */
//	public void runDataObjectListQuery(String engineName, String query) {
//		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
//		Object[] repo = (Object[]) repoList.getSelectedValues();
//		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
//
//		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
//		wrapper.setQuery(query);
//		wrapper.setEngine(engine);
//		wrapper.executeQuery();
//
//		String[] names = wrapper.getVariables();
//		try {
//			while (wrapper.hasNext()) {
//				SesameJenaSelectStatement sjss = wrapper.next();
//				String data = (String) sjss.getVar(names[0]);
//				dataObjectList.add(data);
//				dataObjectBindings += "(<http://health.mil/ontologies/Concept/DataObject/"+data+">)";
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runCostQueries(String engineName, ArrayList<String> queryList) {
		headersList.add("Transition_Cost");
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		
		Hashtable<String,Double> sysAndCostHash = new Hashtable<String,Double>();
		
		for(String query : queryList)
		{
			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
	
			String[] names = wrapper.getVariables();
			try {
				while (wrapper.hasNext()) {
					SesameJenaSelectStatement sjss = wrapper.next();
					String sys = (String) sjss.getVar(names[0]);
					Double cost = (Double) sjss.getVar(names[1]);
					if(sysAndCostHash.containsKey(sys))
						cost +=(Double)sysAndCostHash.get(sys);
					sysAndCostHash.put(sys,cost);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for(String sysKey : sysAndCostHash.keySet())
		{
			if(masterHash.containsKey(sysKey))
			{
				Hashtable sysHash = masterHash.get(sysKey);
				sysHash.put("Transition_Cost", sysAndCostHash.get(sysKey));
			}
		}
	}
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void generateSystemInfoReport() {
		masterHash = new Hashtable<String,Hashtable>();
		sysList = new ArrayList<String>();
		headersList = new ArrayList<String>();
		dataObjectList = new ArrayList<String>();
		dataObjectBindings = "";
		processQueries();
		
		SystemInfoGenWriter writer = new SystemInfoGenWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "System_Info_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		logger.info(fileLoc);	

		writer.exportSystemInfoReport(fileLoc, sysList,headersList,masterHash);
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}

	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processQueries() {

		//System Names
		String sysNameQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
		
		//System Description
		String sysDescriptionQuery = "SELECT DISTINCT ?System ?Full_System_Name ?Description WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Full_System_Name>  ?Full_System_Name}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Description>  ?Description}} ORDER BY ?System";
		
		//System Owners
		String sysOwnersQuery = "SELECT DISTINCT ?System ?System_Owner WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System_Owner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;}{?System ?OwnedBy ?System_Owner}}";

		//System Users
		String sysUsersQuery = "SELECT DISTINCT ?System ?System_User WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;}{?System_User <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>;}{?System ?UsedBy ?System_User}}";

		//System Users
		String sysGarrisonTheaterQuery = "SELECT DISTINCT ?System ?Garrison_Theater WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Garrison_Theater}}";
		
		//Num of Deployment Sites
		String sysNumDeploymentQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DCSite)) as ?Num_Of_Deployment_Sites) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite ?DeployedAt ?DCSite;} {?System ?DeployedAt1 ?SystemDCSite;} } GROUP BY ?System";
		
		//Num of Downstream ICDS
		String sysNumDownstreamICDsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Interface)) AS ?Num_Of_Downstream_Interfaces) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?Upstream ?Interface ;}} GROUP BY ?System";
		
		//Num of Downstream Systems
		String sysNumDownstreamSystemsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DownstreamSys)) AS ?Num_Of_Downstream_Systems) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;}} GROUP BY ?System";
		
		//Num of Upstream ICDS
		String sysNumUpstreamICDsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Interface)) AS ?Num_Of_Upstream_Interfaces) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Interface ?Downstream ?System;}} GROUP BY ?System";
		
		//Num of Upstream Systems
		String sysNumUpstreamSystemsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?UpstreamSys)) AS ?Num_Of_Upstream_Systems) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?System ;}} GROUP BY ?System";
		
		//Num of data objects that this system is a record or source for.
//		String sysNumDataObjectsRecordQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Data)) AS ?Num_Of_Data_Objects_System_Is_Record_Of) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } GROUP BY ?System";
		
//		//Num of data objects that this system is record or source for AND DHMSM creates.
//		String sysNumDataObjectsRecordAndDHMSMCreatedQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Data)) AS ?Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities) WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability;}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task ?Needs ?Data.}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } GROUP BY ?System";
//			
//		//Number of data objects that DHMSM Capabilities create AND are read by this system
//		String sysNumDataObjectsDHMSMCreateAndSystemReadQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Data)) AS ?Num_Of_Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads) WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability;}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}{?Task ?Needs ?Data.} } GROUP BY ?System";
		
		//System Complexity
		String complexityQuery = "SELECT DISTINCT ?System ?Complexity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Rated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Rated>;}{?Complexity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity>;}{?System ?Rated ?Complexity}}";
		
		//System BV and TM
		String BVAndTMQuery = "SELECT DISTINCT ?System (?bv * 100 AS ?Business_Value) (?estm AS ?External_Stability) (?tstm AS ?Technical_Standards_Compliance) (?SustainmentBud AS ?Sustainment_Budget) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv;} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM>  ?estm .} }OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM>  ?tstm .} }OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}  }}";

//		//create list of data objects read by DHMSM
//		String dataObjectsReadByDHMSMQuery = "SELECT DISTINCT ?Data WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability;}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task ?Needs ?Data.}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} }";
//		runDataObjectListQuery(hrCoreEngine,dataObjectsReadByDHMSMQuery);
//		
//		//Num of data objects that this system is a record of and the DHMSM Capabilities read
//		String sysNumDataObjectsRecordAndDHMSMReadQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Data)) AS ?Num_Of_Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } GROUP BY ?System BINDINGS ?Data {@Replace_Bindings@}";
//		sysNumDataObjectsRecordAndDHMSMReadQuery = sysNumDataObjectsRecordAndDHMSMReadQuery.replace("@Replace_Bindings@", dataObjectBindings);
//		
	
		//Transistion Cost queries
		//data consumer
		String dataConsumerQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE { SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Consumer> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;}{?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?data ?input ?GLitem} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} } }GROUP BY ?System";

		//generic data
		String genericDataQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem(ROUND(?loe) AS ?LOEIndividual) WHERE {BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}  {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;}  {?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;}  {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} . {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;}{?System ?provide ?data ;}{?GLitem ?tagged ?gltag;}{?GLitem ?belongs ?phase ;}{?GLitem ?output ?ser ;}{?data ?input ?GLitem} } } GROUP BY ?System";

		//generic blu
		String genericBLUQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?blu ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?provide ?blu ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;}{?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;}{?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?blu ?input ?GLitem} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag). } } GROUP BY ?System";

		//transition data federation
		String dataFedQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;} {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?data ?input ?GLitem} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} } } GROUP BY ?System";

		//blu provider
		String bluProviderQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE {  BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;} {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?blu ?input ?GLitem} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} } } GROUP BY ?System";
		
		ArrayList<String> costQueries = new ArrayList<String>();
		costQueries.add(dataConsumerQuery);
		costQueries.add(genericDataQuery);
		costQueries.add(genericBLUQuery);
		costQueries.add(dataFedQuery);
		costQueries.add(bluProviderQuery);
		
		//run all queries and store them in the masterHash
		runSystemListQuery(hrCoreEngine, sysNameQuery);
		runQuery(hrCoreEngine,sysDescriptionQuery);
		runQuery(hrCoreEngine,sysOwnersQuery);
		runQuery(hrCoreEngine,sysUsersQuery);
		runQuery(hrCoreEngine,sysGarrisonTheaterQuery);
		runQuery(tapSiteEngine,sysNumDeploymentQuery);
		logger.info("Completed Description, Owner/User, and Deployment queries");
		runQuery(hrCoreEngine,sysNumDownstreamICDsQuery);
		runQuery(hrCoreEngine,sysNumDownstreamSystemsQuery);
		runQuery(hrCoreEngine,sysNumUpstreamICDsQuery);
		runQuery(hrCoreEngine,sysNumUpstreamSystemsQuery);
		logger.info("Completed Upstream/Downstream queries");
		
		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		dhelp.runData(engine);

		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of");
		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities");
		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities");
		headersList.add("Num_Of_Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads");
		for(String system : sysList)
		{
			Hashtable sysHash = masterHash.get(system);
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of", dhelp.getAllDataFromSys(system, "C").size());
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities", dhelp.getDataObjectListSupportedFromSystem(system, "C", "C").size());
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities",dhelp.getDataObjectListSupportedFromSystem(system, "C", "R").size());
			sysHash.put("Num_Of_Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads", dhelp.getDataObjectListSupportedFromSystem(system, "R", "C").size());
			masterHash.put(system,sysHash);
		}
		
		runQuery(hrCoreEngine,complexityQuery);
		runQuery(tapCoreEngine,BVAndTMQuery);
		logger.info("Completed Complexity and BV/TM queries");
		runCostQueries(tapCostEngine,costQueries);
		logger.info("Completed Transistion Cost queries");
		


	}

}