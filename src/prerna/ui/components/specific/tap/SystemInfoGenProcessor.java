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
	String tapSiteEngine = "TAP_Site_Data";
	String workingDir = System.getProperty("user.dir");
	Hashtable<String,Hashtable> masterHash;
	ArrayList<String> sysList;
	ArrayList<String> headersList;
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

	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void generateSystemInfoReport() {
		masterHash = new Hashtable<String,Hashtable>();
		sysList = new ArrayList<String>();
		headersList = new ArrayList<String>();
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
		String sysNameQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}} ORDER BY ?System";
		
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
		String sysNumDataObjectsRecordQuery = "SELECT DISTINCT ?System (COUNT(DISTINCT(?Data)) AS ?Num_Of_Data_Objects_System_Is_Source_For) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) } GROUP BY ?System";
		
		//System Complexity
		String complexityQuery = "SELECT DISTINCT ?System ?Complexity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?Rated <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Rated>;}{?Complexity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity>;}{?System ?Rated ?Complexity}}";
		
		//System BV and TM
		String BVAndTMQuery = "SELECT DISTINCT ?System (?bv * 100 AS ?Business_Value) (?estm AS ?External_Stability) (?tstm AS ?Technical_Standards_Compliance) (?SustainmentBud AS ?Sustainment_Budget) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv;} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM>  ?estm .} }OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM>  ?tstm .} }OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}  }}";

		//run all queries and store them in the masterHash
		runSystemListQuery(tapCoreEngine, sysNameQuery);
		runQuery(tapCoreEngine,sysOwnersQuery);
		runQuery(tapCoreEngine,sysUsersQuery);
		runQuery(tapCoreEngine,sysGarrisonTheaterQuery);
		runQuery(tapSiteEngine,sysNumDeploymentQuery);
		runQuery(tapCoreEngine,sysNumDownstreamICDsQuery);
		runQuery(tapCoreEngine,sysNumDownstreamSystemsQuery);
		runQuery(tapCoreEngine,sysNumUpstreamICDsQuery);
		runQuery(tapCoreEngine,sysNumUpstreamSystemsQuery);
		runQuery(tapCoreEngine,sysNumDataObjectsRecordQuery);
		runQuery(tapCoreEngine,complexityQuery);
		runQuery(tapCoreEngine,BVAndTMQuery);
	}

}