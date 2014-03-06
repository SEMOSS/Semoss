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
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the Capability Fact Sheet Reports
 * Used in conjunction with CapabilityFactSheetListener
 */
public class CapabilityFactSheetPerformer {
	Logger logger = Logger.getLogger(getClass());
	String HRCoreEngine = "HR_Core";
	String workingDir = System.getProperty("user.dir");
	CapabilityFactSheetCapDupeCalculator capDupe;
	
	/**
	 * Runs a query on a specific database and returns the result as an ArrayList
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list 		ArrayList<ArrayList<Object>> containing the results of the query 
	 */
	public ArrayList runListQuery(String engineName, String query) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		ArrayList<Object> list = new ArrayList<Object>();
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				for (int colIndex = 0; colIndex < names.length; colIndex++) {
					if (sjss.getVar(names[colIndex]) != null) {
						if (sjss.getVar(names[colIndex]) instanceof Double) {
							list.add(colIndex, (Double) sjss.getVar(names[colIndex]));
						}
						else list.add(colIndex, ((String) sjss.getVar(names[colIndex])).replaceAll("\"",""));						
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(list.size()==0)
			list.add(0.0);
		return list;
	}

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
						else values.add(colIndex, ((String) sjss.getVar(names[colIndex])).replaceAll("\"",""));						
					}
				}
				list.add(values);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}



//	/**
//	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
//	 * @param capabilityName	String containing the capability name to run the queries on
//	 * @return returnHash	Hashtable containing the results for all the queries
//	 */
//	public Hashtable processQueries(String capabilityName) {
//		
//		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	
//		
//		Hashtable<String, Object> firstSheetHash = processFirstSheetQueries(capabilityName);
//		returnHash.put("CapabilityOverviewSheet", firstSheetHash);
//		
//		Hashtable<String, Object> capabilityDupeSheetHash = processCapabilityDupeSheet(capabilityName);
//		returnHash.put("CapabilityDupeSheet", capabilityDupeSheetHash);
//
//		Hashtable<String, Object> dataSheet = processDataSheetQueries(capabilityName);
//		returnHash.put("DataSheet", dataSheet);
//		
//		Hashtable<String, Object> bluSheet = processBLUSheetQueries(capabilityName);
//		returnHash.put("BLUSheet", bluSheet);
//		
//		Hashtable<String, Object> funtionalGapSheet = processFunctionalGapSheetQueries(capabilityName);
//		returnHash.put("FunctionalGapSheet", funtionalGapSheet);
//		
//		return returnHash;
//	}

	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processFirstSheetQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	
		
		//Capability and Capability Group
		String capabilityGroupQuery = "SELECT DISTINCT ?Capability ?CapabilityObjective ?CapabilityGroup ?CapabilityDescription WHERE {BIND( <http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability){?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityGroup ?Consists ?Capability}OPTIONAL{?Capability <http://semoss.org/ontologies/Relation/Contains/Objective> ?CapabilityObjective}OPTIONAL{?CapabilityGroup <http://semoss.org/ontologies/Relation/Contains/Description> ?CapabilityDescription}}";
		capabilityGroupQuery = capabilityGroupQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
	
		//Mission Outcome
		String missionOutcomeQuery = "SELECT DISTINCT ?MissionOutcome WHERE {BIND( <http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability){?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityGroup ?Consists ?Capability}{?MissionOutcome <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MissionOutcome>;} {?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?MissionOutcome ?Utilizes ?CapabilityGroup}}";
		capabilityGroupQuery = capabilityGroupQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//CONOPS Source
		String conopsSourceQuery = "SELECT DISTINCT ?CONOPSSource WHERE {BIND( <http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability){?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityGroup ?Consists ?Capability}{?CONOPSSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;} {?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CONOPSSource ?Utilizes ?CapabilityGroup}}";
		capabilityGroupQuery = capabilityGroupQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
			
		//Task Count
		String taskCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?Task)) AS ?TaskCount)  WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}}";		
		taskCountQuery = taskCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);

		//BP Count
		String bpCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?BusinessProcess)) AS ?BusinessProcessCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?Capability ?Consists ?Task.} {?Task ?Needs ?BusinessProcess}}";		
		bpCountQuery = bpCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//BR Count
		String brCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?BusinessRule)) AS ?BusinessRuleCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?BusinessRule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessRule>; }{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>; }{?Capability ?Consists ?Task.}{?Task ?Supports ?BusinessRule.} }";		
		brCountQuery = brCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//BS Count
		String bsCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?BusinessStandard)) AS ?BusinessStandardCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?BusinessStandard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessStandard>; }{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>; }{?Capability ?Consists ?Task.}{?Task ?Supports ?BusinessStandard.} }";		
		bsCountQuery = bsCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//TR Count
		String trCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?TechRequirement)) AS ?TechRequirementCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>; }{?Attribute  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute>; }  {?Satisfies <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Satisfies>; }{?TechRequirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechRequirement>}{?TechSubCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechSubCategory>}{?TechSubCategory_Has_TechRequirement <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?TechCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechCategory>}{?TechCategory_Has_TechSubCategory <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?Capability ?Consists ?Task.}{?Task ?Has ?Attribute.}{?TechCategory ?TechCategory_Has_TechSubCategory ?TechSubCategory}{?TechSubCategory ?TechSubCategory_Has_TechRequirement ?TechRequirement}{?TechRequirement ?Satisfies ?Attribute.}}";		
		trCountQuery = trCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//TS Count
		String tsCountQuery = "SELECT DISTINCT	(COUNT(DISTINCT(?TechStandard)) AS ?TechStandardCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>; }{?Attribute  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute>; }  {?Satisfies <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Satisfies>; }{?TechRequirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechRequirement>}  {?TechStandard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechStandardIdentifier>; } {?TechRequirement_Has_TechStandard <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}  {?TechSubCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechSubCategory>}{?TechSubCategory_Has_TechRequirement <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?TechCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechCategory>}{?TechCategory_Has_TechSubCategory <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?Capability ?Consists ?Task.}{?Task ?Has ?Attribute.}{?TechCategory ?TechCategory_Has_TechSubCategory ?TechSubCategory}{?TechSubCategory ?TechSubCategory_Has_TechRequirement ?TechRequirement}{?TechRequirement ?Satisfies ?Attribute.}{?TechRequirement ?TechRequirement_Has_TechStandard ?TechStandard}}";		
		tsCountQuery = tsCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//Data Count --> Assuming R means Consume and C means provide
		String dataCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?Data)) AS ?DataCount) (SUM(IF(?CRM = 'R', 1, 0)) AS ?ConsumeCount)(SUM(IF(?CRM = 'C', 1, 0)) AS ?ProvideCount) WHERE {SELECT DISTINCT ?Data ?CRM WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.} }}";		
		dataCountQuery = dataCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//BLU Count
		String bluCountQuery = "SELECT DISTINCT	(COUNT(DISTINCT(?BusinessLogicUnit)) AS ?BLUCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}}";
		bluCountQuery = bluCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//Participants
		String participantQuery = "SELECT DISTINCT ?Participant WHERE { BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;}{?Participant <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Participant>;}{?Capability ?Consists ?Task.} {?Task ?Requires ?Participant.}}";
		participantQuery = participantQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//System Count
		String systemCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?System)) AS ?SystemCount) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Provides <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?Provides <http://semoss.org/ontologies/Relation/Contains/CRM> 'C';}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.}{?System ?Provides ?Data}}";			
		systemCountQuery = systemCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> capabilityGroupResultsList = runListQuery(HRCoreEngine, capabilityGroupQuery);
		ArrayList<Object> missionOutcomeResultsList = runListQuery(HRCoreEngine, missionOutcomeQuery);
		ArrayList<Object> conopsSourceResultsList = runListQuery(HRCoreEngine, conopsSourceQuery);
		ArrayList<Object> taskCountResultsList = runListQuery(HRCoreEngine, taskCountQuery);
		ArrayList<Object> bpCountResultsList = runListQuery(HRCoreEngine, bpCountQuery);
		ArrayList<Object> brCountResultsList = runListQuery(HRCoreEngine, brCountQuery);
		ArrayList<Object> bsCountResultsList = runListQuery(HRCoreEngine, bsCountQuery);
		ArrayList<Object> trCountResultsList = runListQuery(HRCoreEngine, trCountQuery);
		ArrayList<Object> tsCountResultsList = runListQuery(HRCoreEngine, tsCountQuery);
		ArrayList<Object> dataCountResultsList = runListQuery(HRCoreEngine, dataCountQuery);
		ArrayList<Object> bluCountResultsList = runListQuery(HRCoreEngine, bluCountQuery);
		ArrayList<Object> systemCountResultsList = runListQuery(HRCoreEngine,systemCountQuery);
		ArrayList<Object> participantResultsList = runListQuery(HRCoreEngine, participantQuery);

		returnHash.put(ConstantsTAP.CAPABILITY_GROUP_QUERY, capabilityGroupResultsList);
		returnHash.put(ConstantsTAP.MISSION_OUTCOME_QUERY, missionOutcomeResultsList);
		returnHash.put(ConstantsTAP.CONOPS_SOURCE_QUERY, conopsSourceResultsList);
		returnHash.put(ConstantsTAP.TASK_COUNT_QUERY, taskCountResultsList);
		returnHash.put(ConstantsTAP.BP_COUNT_QUERY, bpCountResultsList);
		returnHash.put(ConstantsTAP.BR_COUNT_QUERY, brCountResultsList);
		returnHash.put(ConstantsTAP.BS_COUNT_QUERY, bsCountResultsList);
		returnHash.put(ConstantsTAP.TR_COUNT_QUERY, trCountResultsList);	
		returnHash.put(ConstantsTAP.TS_COUNT_QUERY, tsCountResultsList);
		returnHash.put(ConstantsTAP.DATA_COUNT_QUERY, dataCountResultsList);
		returnHash.put(ConstantsTAP.BLU_COUNT_QUERY, bluCountResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_COUNT_QUERY, systemCountResultsList);
		returnHash.put(ConstantsTAP.PARTICIPANT_QUERY, participantResultsList);
	
		//add in date generated
		String dateGenerated = DateFormat.getDateInstance(DateFormat.SHORT).format(new Date());
		ArrayList<Object> dateGeneratedResultsList = new ArrayList<Object>();
		dateGeneratedResultsList.add(dateGenerated);
		returnHash.put(ConstantsTAP.DATE_GENERATED_QUERY, dateGeneratedResultsList);
				
		return returnHash;
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processCapabilityDupeSheet(String capabilityName) {
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		capDupe = new CapabilityFactSheetCapDupeCalculator();
		
		ArrayList<ArrayList<Object>> capDupeList = capDupe.priorityAllDataHash.get(capabilityName);
		ArrayList<String> capList = capDupe.priorityCapHash.get(capabilityName);
		ArrayList<Double> valueList = capDupe.priorityValueHash.get(capabilityName);
		ArrayList<String> criteriaList = capDupe.criteriaList;

		Hashtable dataSeries = new Hashtable();
		for(int i=0;i<capList.size();i++)
		{
			String capability = capList.get(i);
			for(int j=0;j<criteriaList.size();j++)
			{
				String criteria =criteriaList.get(j);
				Hashtable elementHash = new Hashtable();
				elementHash.put("Capability", capability);
				elementHash.put("Criteria", criteria);
				elementHash.put("val",capDupeList.get(i).get(j));
				dataSeries.put(capability+"-"+criteria,elementHash);
			}
			String criteria ="Overall";
			Hashtable elementHash = new Hashtable();
			elementHash.put("Capability", capability);
			elementHash.put("Criteria", criteria);
			elementHash.put("val",(valueList.get(i)));
			dataSeries.put(capability+"-"+criteria,elementHash);
		}
		
		returnHash.put("dataSeries",dataSeries);
		returnHash.put("value", "val");
		returnHash.put("xAxisTitle", "Capability");
		returnHash.put("yAxisTitle", "Criteria");
		returnHash.put("title", "Capability Similarity / Duplication Scores");

		return returnHash;
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processDataSheetQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		String dataObjectQuery = "SELECT DISTINCT ?Data ?CRM WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.}}";		
		dataObjectQuery = dataObjectQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> dataObjectResultsList = runQuery(HRCoreEngine, dataObjectQuery);

		returnHash.put(ConstantsTAP.DATE_OBJECT_QUERY, dataObjectResultsList);
	
		return returnHash;
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processBLUSheetQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		String bluObjectQuery = "SELECT DISTINCT ?BusinessLogicUnit WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}}";		
		bluObjectQuery = bluObjectQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> bluObjectResultsList = runQuery(HRCoreEngine, bluObjectQuery);

		returnHash.put(ConstantsTAP.BLU_QUERY, bluObjectResultsList);
	
		return returnHash;
	}

	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processFunctionalGapSheetQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		String functionalGapQuery = "SELECT DISTINCT (COALESCE(?FError1,?FError2) AS ?FError) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability) {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?FError1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}{?FError1 ?FError_Needs_BusinessLogicUnit ?BusinessLogicUnit} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Task_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 ?FError_Needs_Data ?Data} }}";		
		functionalGapQuery = functionalGapQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> functionalGapResultsList = runQuery(HRCoreEngine, functionalGapQuery);

		returnHash.put(ConstantsTAP.FUNCTIONAL_GAP_QUERY, functionalGapResultsList);
	
		return returnHash;
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processSystemQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		String systemConsumeQuery = "SELECT DISTINCT (COALESCE(?FError1,?FError2) AS ?FError) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability) {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?FError1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}{?FError1 ?FError_Needs_BusinessLogicUnit ?BusinessLogicUnit} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Task_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 ?FError_Needs_Data ?Data} }}";		
		systemConsumeQuery = systemConsumeQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		String systemProvideQuery = "SELECT DISTINCT (COALESCE(?FError1,?FError2) AS ?FError) WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability) {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?FError1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}{?FError1 ?FError_Needs_BusinessLogicUnit ?BusinessLogicUnit} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Task_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>} {?FError_Needs_Data <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data ?Data}{?FError2 ?FError_Needs_Data ?Data} }}";		
		systemProvideQuery = systemProvideQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> systemConsumeResultsList = runQuery(HRCoreEngine, systemConsumeQuery);
		ArrayList<Object> systemProvideeResultsList = runQuery(HRCoreEngine, systemProvideQuery);

		returnHash.put(ConstantsTAP.SYSTEM_CONSUME_DATA_QUERY, systemConsumeResultsList);
		returnHash.put(ConstantsTAP.SYSTEM_PROVIDE_DATA_QUERY, systemProvideeResultsList);
	
		return returnHash;
	}
}