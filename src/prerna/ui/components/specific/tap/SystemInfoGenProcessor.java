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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.BasicReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the System Info Report
 * Used in conjunction with SystemInfoGenListener
 */
public class SystemInfoGenProcessor {
	static final Logger logger = LogManager.getLogger(SystemInfoGenProcessor.class.getName());
	String tapCostEngine = "TAP_Cost_Data";
	String tapSiteEngine = "TAP_Site_Data";
	String hrCoreEngine = "HR_Core";
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	Hashtable<String,Hashtable> masterHash;
	ArrayList<String> sysList;
	ArrayList<String> capList;
	ArrayList<String> headersList;
	ArrayList<String> dataObjectList;
	String dataObjectBindings;
	FactSheetSysSimCalculator sysSim;
	/**
	 * Runs a query on a specific database and puts the results in the masterHash for a system
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runQuery(String engineName, String query) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		try{
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		for(String head : names)
			if(!headersList.contains(head))
				headersList.add(head);

			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				if(masterHash.containsKey(sys))
				{
					Hashtable<String, Object> sysHash = (Hashtable<String, Object>)masterHash.get(sys);
					for (int colIndex = 1; colIndex < names.length; colIndex++) {
						String varName = names[colIndex];
						Object val = sjss.getVar(names[colIndex]);
						if (val != null) {
							if (val instanceof Double) {
								sysHash.put(varName, val);
							}
							else
							{
//								if(varName.contains("Logic")&&varName.contains("Num"))
//								{
//									if(sysHash.containsKey(varName))
//										sysHash.put(varName,(Double)sysHash.get(varName)+1);
//									else
//										sysHash.put(varName, )
//								}
//								else{
									if(sysHash.containsKey(varName))
										val = (String) sysHash.get(varName) +", " + (String) val;
									sysHash.put(varName, val);
//								}
							}
						}
					}
				}
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
	}
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runSystemListQuery(String engineName, String query) {
		try {

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();

			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				Hashtable sysHash = new Hashtable();
				masterHash.put((String) sjss.getVar(names[0]),sysHash);
				sysList.add((String) sjss.getVar(names[0]));
			}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
	}
	
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runCostQueries(String engineName, ArrayList<String> queryList) {
		try{
			headersList.add("Transition_Cost");
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
				} catch (RuntimeException e) {
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
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
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

		//checking to see if databases are loaded
		String engineName = tapSiteEngine;
		try
		{
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			if(engine==null)
				throw new NullPointerException();
			engineName = tapCostEngine;
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			if(engine==null)
				throw new NullPointerException();
			engineName = hrCoreEngine;
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			if(engine==null)
				throw new NullPointerException();
			
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
			return;
		}
		
		sysSim = new FactSheetSysSimCalculator();	
		
		processQueries();
		
		BasicReportWriter writer = new BasicReportWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "System_Info_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		writer.makeWorkbook(fileLoc);
		logger.info(fileLoc);
		
		ArrayList<Object[]> masterList = writer.makeListFromHash(headersList, sysList, masterHash);

		writer.writeListSheet("System Info",headersList,masterList);
		writer.writeWorkbook();
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}

	
	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processQueries() {

		//System Names
		String sysNameQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}}ORDER BY ?System";// BINDINGS ?User {(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)}";
		
		//System Description
		String sysDescriptionQuery = "SELECT DISTINCT ?System ?Full_System_Name ?Description WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Full_System_Name>  ?Full_System_Name}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Description>  ?Description}} ORDER BY ?System";
		
		//System Owners
		String sysOwnersQuery = "SELECT DISTINCT ?System ?System_Owner WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System_Owner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;}{?System ?OwnedBy ?System_Owner}} ORDER BY ?System ?System_Owner";

		//System Users
		String sysUsersQuery = "SELECT DISTINCT ?System ?System_User WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;}{?System_User <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>;}{?System ?UsedBy ?System_User}} ORDER BY ?System ?System_User";

		//Garrison Theater
		String sysGarrisonTheaterQuery = "SELECT DISTINCT ?System ?Garrison_Theater WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Garrison_Theater}}";
		
		//Num of Deployment Sites
		String sysNumDeploymentQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DCSite)) as ?Num_Of_Deployment_Sites) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite ?DeployedAt ?DCSite;} {?System ?DeployedAt1 ?SystemDCSite;} } GROUP BY ?System";
		
		//Deployment Sites
		String sysDeploymentQuery = "SELECT DISTINCT ?System ?Site WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite ?DeployedAt ?Site;} {?System ?DeployedAt1 ?SystemDCSite;} }";
		
		//Num of Downstream ICDS
		String sysNumDownstreamICDsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Interface)) AS ?Num_Of_Downstream_Interfaces) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?Upstream ?Interface ;}} GROUP BY ?System";
		
		//Num of Downstream Systems
		String sysNumDownstreamSystemsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DownstreamSys)) AS ?Num_Of_Downstream_Systems) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;}} GROUP BY ?System";
		
		//Num of Upstream ICDS
		String sysNumUpstreamICDsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Interface)) AS ?Num_Of_Upstream_Interfaces) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Interface ?Downstream ?System;}} GROUP BY ?System";
		
		//Num of Upstream Systems
		String sysNumUpstreamSystemsQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?UpstreamSys)) AS ?Num_Of_Upstream_Systems) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?System ;}} GROUP BY ?System";
		
		//System Providing Business Process Queries
		String sysNumBPQuery = "SELECT DISTINCT ?System (COUNT(?bp) AS ?Num_Of_Business_Processes_System_Supports) WHERE{SELECT DISTINCT ?System ?bp WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?supportsBP <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?System ?supportsBP ?bp}}} GROUP BY ?System";

		String sysBPQuery = "SELECT DISTINCT ?System ?Business_Processes_System_Supports WHERE {{?Business_Processes_System_Supports <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?supportsBP <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?System ?supportsBP ?Business_Processes_System_Supports}}";
		
		//System Providing Activity Queries
		String sysNumActivityQuery = "SELECT DISTINCT ?System (COUNT(?act) AS ?Num_Of_Activities_System_Supports) WHERE{SELECT DISTINCT ?System ?act WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?supportsActivity <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?System ?supportsActivity ?act}}} GROUP BY ?System";

		String sysActivityQuery = "SELECT DISTINCT ?System ?Activities_System_Supports WHERE {{?Activities_System_Supports <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?supportsActivity <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?System ?supportsActivity ?Activities_System_Supports}}";
		
		//System Providing BLU Queries
		String sysNumBLUQuery = "SELECT DISTINCT ?System (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides) WHERE{SELECT DISTINCT ?System ?blu WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu}}} GROUP BY ?System";

		String sysBLUQuery = "SELECT DISTINCT ?System ?Business_Logic_Units_System_Provides WHERE {{?Business_Logic_Units_System_Provides <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?Business_Logic_Units_System_Provides}}";

		//System and DHMSM Providing BLU Queries
		String sysAndDHMSMNumBLUQuery = "SELECT DISTINCT ?System (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities) WHERE{SELECT DISTINCT ?System ?blu WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?blu}{?System ?provideBLU ?blu}}} GROUP BY ?System";
		
		String sysAndDHMSMBLUQuery = "SELECT DISTINCT ?System ?Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities}{?System ?provideBLU ?Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities}}";
		
		String sysAndNotDHMSMBLUCountQuery = "SELECT DISTINCT ?System (COUNT(?blu) AS ?Num_Of_Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities) WHERE{SELECT DISTINCT ?System ?blu WHERE {{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?blu}}FILTER(!BOUND(?cap))}} GROUP BY ?System";
		
		String sysAndNotDHMSMBLUQuery = "SELECT DISTINCT ?System ?Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities WHERE {{?Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> } {?task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap} {?cap <http://semoss.org/ontologies/Relation/Consists> ?task} {?task <http://semoss.org/ontologies/Relation/Needs> ?Business_Logic_Units_System_Provides_And_Not_Provided_By_DHMSM_Capabilities}}FILTER(!BOUND(?cap))}";
				
//		//System Complexity
//		String complexityQuery = "SELECT DISTINCT ?System ?Complexity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Rated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Rated>;}{?Complexity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity>;}{?System ?Rated ?Complexity}}";
//		
//		//System BV and TM
//		//String BVAndTMQuery = "SELECT DISTINCT ?System (?bv * 100 AS ?Business_Value) (?estm AS ?External_Stability) (?tstm AS ?Technical_Standards_Compliance) (?SustainmentBud AS ?Sustainment_Budget) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv;} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM>  ?estm .} }OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM>  ?tstm .} }OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}  }}";
//		String BVAndTMQuery = "SELECT DISTINCT ?System (?bv * 100 AS ?Business_Value) (?estm AS ?External_Stability) (?tstm AS ?Technical_Standards_Compliance) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv;} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM>  ?estm .} }OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM>  ?tstm .} }}";
//		
//		//Sustainment Budget
//		String sustainmentQuery = "SELECT DISTINCT ?System (SUM(?value) AS ?Sustainment_Budget) WHERE{ SELECT DISTINCT ?System ?budget ?value  WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?budget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?System ?has ?budget .} {?budget <http://semoss.org/ontologies/Relation/Contains/Cost> ?value ;} } } GROUP BY ?System";
//
//		//Transition Cost queries
//		//data consumer
//		String dataConsumerQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE { SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Consumer> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;}{?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?data ?input ?GLitem} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} } }GROUP BY ?System";
//
//		//generic data
//		String genericDataQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem(ROUND(?loe) AS ?LOEIndividual) WHERE {BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}  {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;}  {?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;}  {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} . {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;}{?System ?provide ?data ;}{?GLitem ?tagged ?gltag;}{?GLitem ?belongs ?phase ;}{?GLitem ?output ?ser ;}{?data ?input ?GLitem} } } GROUP BY ?System";
//
//		//generic blu
//		String genericBLUQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?blu ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?provide ?blu ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;}{?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;}{?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?blu ?input ?GLitem} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag). } } GROUP BY ?System";
//
//		//transition data federation
//		String dataFedQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;} {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?data ?input ?GLitem} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} } } GROUP BY ?System";
//
//		//blu provider
//		String bluProviderQuery = "SELECT DISTINCT ?System (SUM(?LOEIndividual) AS ?LOE) WHERE {SELECT DISTINCT ?System ?phase ?data ?GLitem (ROUND(?loe) AS ?LOEIndividual) WHERE {  BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?tagged <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?GLitem ?tagged ?gltag;} {?influences <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Influences>;}{?System ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?GLitem ?belongs ?phase ;} {?output <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Output>;} {?GLitem ?output ?ser ;}{?input <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Input>;} {?blu ?input ?GLitem} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} } } GROUP BY ?System";
		
//		ArrayList<String> costQueries = new ArrayList<String>();
//		costQueries.add(dataConsumerQuery);
//		costQueries.add(genericDataQuery);
//		costQueries.add(genericBLUQuery);
//		costQueries.add(dataFedQuery);
//		costQueries.add(bluProviderQuery);
		
		//run all queries and store them in the masterHash
		runSystemListQuery(hrCoreEngine, sysNameQuery);
		runQuery(hrCoreEngine,sysDescriptionQuery);
		runQuery(hrCoreEngine,sysOwnersQuery);
		runQuery(hrCoreEngine,sysUsersQuery);
		headersList.add("Probability");
		headersList.add("Percentage_Of_Data_Objects_Covered_By_DHMSM");
		headersList.add("Percentage_Of_Business_Logic_Units_Covered_By_DHMSM");
		runQuery(hrCoreEngine,sysGarrisonTheaterQuery);
		runQuery(tapSiteEngine,sysNumDeploymentQuery);
		runQuery(tapSiteEngine,sysDeploymentQuery);
		logger.info("Completed Description, Owner/User, and Deployment queries");
		runQuery(hrCoreEngine,sysNumDownstreamICDsQuery);
		runQuery(hrCoreEngine,sysNumDownstreamSystemsQuery);
		runQuery(hrCoreEngine,sysNumUpstreamICDsQuery);
		runQuery(hrCoreEngine,sysNumUpstreamSystemsQuery);
		logger.info("Completed Upstream/Downstream queries");
		
		runQuery(hrCoreEngine,sysNumBPQuery);
		runQuery(hrCoreEngine,sysBPQuery);
		runQuery(hrCoreEngine,sysNumActivityQuery);
		runQuery(hrCoreEngine,sysActivityQuery);
		logger.info("Completed business process/activities queries");

		
		
		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		dhelp.runData(engine);

		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of");
		headersList.add("Data_Objects_System_Is_Record_Of");
		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities");
		headersList.add("Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities");
		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of_And_Not_Created_By_DHMSM_Capabilities");
		headersList.add("Data_Objects_System_Is_Record_Of_And_Not_Created_By_DHMSM_Capabilities");
//		headersList.add("Num_Of_Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities");
//		headersList.add("Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities");
//		headersList.add("Num_Of_Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads");
//		headersList.add("Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads");

		runQuery(hrCoreEngine,sysNumBLUQuery);
		runQuery(hrCoreEngine,sysBLUQuery);
		runQuery(hrCoreEngine,sysAndDHMSMNumBLUQuery);
		runQuery(hrCoreEngine,sysAndDHMSMBLUQuery);
		runQuery(hrCoreEngine,sysAndNotDHMSMBLUCountQuery);
		runQuery(hrCoreEngine,sysAndNotDHMSMBLUQuery);

	
//		headersList.add("Top_Five_Similar_Systems");
		for(String system : sysList)
		{
			Hashtable sysHash = masterHash.get(system);

			ArrayList<String> dataSystemSORList = dhelp.getAllDataFromSys(system, "C");
			int numOfDataSystemSOR = dataSystemSORList.size();
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of", numOfDataSystemSOR);
			sysHash.put("Data_Objects_System_Is_Record_Of", makeConcatString(dataSystemSORList));
			ArrayList<String> dataSystemSORAndDHMSMCreateList = dhelp.getDataObjectListSupportedFromSystem(system, "C", "C");
			int numOfDataSystemSORAndDHMSMCreate = dataSystemSORAndDHMSMCreateList.size();
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities", numOfDataSystemSORAndDHMSMCreate);
			sysHash.put("Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities", makeConcatString(dataSystemSORAndDHMSMCreateList));
//			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities",dhelp.getDataObjectListSupportedFromSystem(system, "C", "R").size());
//			sysHash.put("Data_Objects_System_Is_Record_Of_And_Read_By_DHMSM_Capabilities",makeConcatString(dhelp.getDataObjectListSupportedFromSystem(system, "C", "R")));
//			sysHash.put("Num_Of_Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads", dhelp.getDataObjectListSupportedFromSystem(system, "R", "C").size());
//			sysHash.put("Data_Objects_DHMSM_Capabilities_Create_And_This_System_Reads", makeConcatString(dhelp.getDataObjectListSupportedFromSystem(system, "R", "C")));
			sysHash.put("Num_Of_Data_Objects_System_Is_Record_Of_And_Not_Created_By_DHMSM_Capabilities", numOfDataSystemSOR-numOfDataSystemSORAndDHMSMCreate);
			sysHash.put("Data_Objects_System_Is_Record_Of_And_Not_Created_By_DHMSM_Capabilities", makeConcatString(getNotCoveredList(dataSystemSORList,dataSystemSORAndDHMSMCreateList)));
//			ArrayList<String> sysSimList = sysSim.prioritySysHash.get(system);
//			ArrayList<Double> sysSimValueList = sysSim.priorityValueHash.get(system);
//			String sysSimConcat = "";
//			if(sysSimList!=null)
//			{
//				if(sysSimList.size()>0)
//					sysSimConcat = sysSimList.get(0)+": "+sysSimValueList.get(0)*100+"%";
//				for(int i=1;i<sysSimList.size();i++)
//					sysSimConcat +=", "+sysSimList.get(i)+": "+sysSimValueList.get(i)*100+"%";
//			}
//			sysHash.put("Top_Five_Similar_Systems",sysSimConcat);
//			masterHash.put(system,sysHash);
		}
		
		runProbability(dhelp);
//		runQuery(hrCoreEngine,complexityQuery);
//		runQuery(tapCoreEngine,BVAndTMQuery);
//		runQuery(tapCostEngine,sustainmentQuery);
//		logger.info("Completed Complexity and BV/TM queries");
//		runCostQueries(tapCostEngine,costQueries);
//		logger.info("Completed Transistion Cost queries");
		
	}
	public String makeConcatString(ArrayList<String> vals)
	{
		String concat = "";
		if(vals==null||vals.size()==0)
			return concat;
		concat = vals.get(0);
		for(int i=1;i<vals.size();i++)
			concat+=", "+vals.get(i);
		return concat;
	}
	public ArrayList<String> getNotCoveredList(ArrayList<String> systemList, ArrayList<String> systemAndDHMSMList)
	{
		ArrayList<String> retVal = new ArrayList<String>();
		for(String data : systemList)
		{
			if(!systemAndDHMSMList.contains(data))
				retVal.add(data);
		}
		return retVal;
	}
	
	public void runProbability(DHMSMHelper dhelp)
	{
		for(String system : sysList)
		{
			Hashtable sysHash = masterHash.get(system);
			int numOfDataSystemSOR = (Integer) sysHash.get("Num_Of_Data_Objects_System_Is_Record_Of");
			int numOfDataSystemSORAndDHMSMCreate = (Integer) sysHash.get("Num_Of_Data_Objects_System_Is_Record_Of_And_Created_By_DHMSM_Capabilities");
			
			double numOfBLUSystemSOR = 0;
			double numOfBLUSystemSORAndDHMSMCreate = 0;
			if(sysHash.containsKey("Num_Of_Business_Logic_Units_System_Provides"))
				numOfBLUSystemSOR= (Double) sysHash.get("Num_Of_Business_Logic_Units_System_Provides");
			if(sysHash.containsKey("Num_Of_Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities"))
				numOfBLUSystemSORAndDHMSMCreate = (Double) sysHash.get("Num_Of_Business_Logic_Units_System_Provides_And_Provided_By_DHMSM_Capabilities");
			if(numOfBLUSystemSOR==0)
				sysHash.put("Percentage_Of_Business_Logic_Units_Covered_By_DHMSM", "");
			else if(numOfBLUSystemSORAndDHMSMCreate == 0)
				sysHash.put("Percentage_Of_Business_Logic_Units_Covered_By_DHMSM", 0);
			else if(numOfBLUSystemSOR==numOfBLUSystemSORAndDHMSMCreate)
				sysHash.put("Percentage_Of_Business_Logic_Units_Covered_By_DHMSM", 100);
			else
			{
				double bluPercent = (double)numOfBLUSystemSORAndDHMSMCreate/ (double)numOfBLUSystemSOR;
				sysHash.put("Percentage_Of_Business_Logic_Units_Covered_By_DHMSM", bluPercent*100);				
			}
			
			if(numOfDataSystemSOR == 0)
			{
				sysHash.put("Probability", "Question");
				sysHash.put("Percentage_Of_Data_Objects_Covered_By_DHMSM", "");
			}
			else if((numOfDataSystemSOR == numOfDataSystemSORAndDHMSMCreate))
			{
				sysHash.put("Probability", "High");
				sysHash.put("Percentage_Of_Data_Objects_Covered_By_DHMSM", 100);
			}
			else if(numOfDataSystemSORAndDHMSMCreate ==0)
			{
				sysHash.put("Probability", "Low");
				sysHash.put("Percentage_Of_Data_Objects_Covered_By_DHMSM", 0);
			}
			else
			{
				double dataPercent = (double)numOfDataSystemSORAndDHMSMCreate/ (double)numOfDataSystemSOR;
				sysHash.put("Probability", "Medium");
				sysHash.put("Percentage_Of_Data_Objects_Covered_By_DHMSM", dataPercent*100);
			}
		}
	}
	


}