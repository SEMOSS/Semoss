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
	Hashtable masterHash = new Hashtable();
	
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
						else if(((String)sjss.getVar(names[colIndex])).length()>0)
							list.add(colIndex, ((String) sjss.getVar(names[colIndex])).replaceAll("\"",""));
						else
							list.add(colIndex, "N/A");
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
				
		//Participants
		String participantQuery = "SELECT DISTINCT ?Participant WHERE { BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;}{?Participant <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Participant>;}{?Capability ?Consists ?Task.} {?Task ?Requires ?Participant.}}";
		participantQuery = participantQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
			
		//Data Count --> Assuming R means Consume and C means provide
		String dataCountQuery = "SELECT DISTINCT (COUNT(DISTINCT(?Data)) AS ?DataCount) (SUM(IF(?CRM = 'R', 1, 0)) AS ?ConsumeCount)(SUM(IF(?CRM = 'C', 1, 0)) AS ?ProvideCount) WHERE {SELECT DISTINCT ?Data ?CRM WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.} }}";		
		dataCountQuery = dataCountQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> capabilityGroupResultsList = runListQuery(HRCoreEngine, capabilityGroupQuery);
		ArrayList<Object> missionOutcomeResultsList = runListQuery(HRCoreEngine, missionOutcomeQuery);
		ArrayList<Object> conopsSourceResultsList = runListQuery(HRCoreEngine, conopsSourceQuery);
		ArrayList<Object> participantResultsList = runListQuery(HRCoreEngine, participantQuery);
		ArrayList<Object> dataCountResultsList = runListQuery(HRCoreEngine, dataCountQuery);

		ArrayList<Object> taskResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.TASK_QUERY);
		ArrayList<Object> taskCountResultsList = new ArrayList<Object>();
		if(taskResultsList!=null)
			taskCountResultsList.add(taskResultsList.size());
		else
			taskCountResultsList.add(0);
		ArrayList<Object> bpResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.BP_QUERY);
		ArrayList<Object> bpCountResultsList = new ArrayList<Object>();
		if(bpResultsList!=null)
			bpCountResultsList.add(bpResultsList.size());
		else
			bpCountResultsList.add(0);
		ArrayList<Object> brResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.BR_QUERY);
		ArrayList<Object> brCountResultsList = new ArrayList<Object>();
		if(brResultsList!=null)
			brCountResultsList.add(brResultsList.size());
		else
			brCountResultsList.add(0);
		ArrayList<Object> bsResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.BS_QUERY);
		ArrayList<Object> bsCountResultsList = new ArrayList<Object>();
		if(bsResultsList!=null)
			bsCountResultsList.add(bsResultsList.size());
		else
			bsCountResultsList.add(0);
		ArrayList<Object> trResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.TR_QUERY);
		ArrayList<Object> trCountResultsList = new ArrayList<Object>();
		if(trResultsList!=null)
			trCountResultsList.add(trResultsList.size());
		else
			trCountResultsList.add(0);
		ArrayList<Object> tsResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.TS_QUERY);
		ArrayList<Object> tsCountResultsList = new ArrayList<Object>();
		if(tsResultsList!=null)
			tsCountResultsList.add(tsResultsList.size());
		else
			tsCountResultsList.add(0);
		ArrayList<Object> bluResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.BLU_QUERY);
		ArrayList<Object> bluCountResultsList = new ArrayList<Object>();
		if(bluResultsList!=null)
			bluCountResultsList.add(bluResultsList.size());
		else
			bluCountResultsList.add(0);	
		ArrayList<Object> functionalGapResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.FUNCTIONAL_GAP_QUERY);
		ArrayList<Object> functionalGapCountResultsList = new ArrayList<Object>();
		if(functionalGapResultsList!=null)
			functionalGapCountResultsList.add(functionalGapResultsList.size());
		else
			functionalGapCountResultsList.add(0);
		ArrayList<Object> capProvideSystemProvideResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.CAP_PROVIDE_SYSTEM_PROVIDE_DATA_QUERY);
		ArrayList<Object> capProvideSystemProvideCountResultsList = new ArrayList<Object>();
		if(capProvideSystemProvideResultsList!=null)
			capProvideSystemProvideCountResultsList.add(capProvideSystemProvideResultsList.size());
		else
			capProvideSystemProvideCountResultsList.add(0);
		ArrayList<Object> capProvideSystemConsumeResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.CAP_PROVIDE_SYSTEM_CONSUME_DATA_QUERY);
		ArrayList<Object> capProvideSystemConsumeCountResultsList = new ArrayList<Object>();
		if(capProvideSystemConsumeResultsList!=null)
			capProvideSystemConsumeCountResultsList.add(capProvideSystemConsumeResultsList.size());
		else
			capProvideSystemConsumeCountResultsList.add(0);
		ArrayList<Object> capConsumeSystemProvideResultsList = (ArrayList<Object>)masterHash.get(ConstantsTAP.CAP_CONSUME_SYSTEM_PROVIDE_DATA_QUERY);
		ArrayList<Object> capConsumeSystemProvideCountResultsList = new ArrayList<Object>();
		if(capConsumeSystemProvideResultsList!=null)
			capConsumeSystemProvideCountResultsList.add(capConsumeSystemProvideResultsList.size());
		else
			capConsumeSystemProvideCountResultsList.add(0);
	
		returnHash.put(ConstantsTAP.CAPABILITY_GROUP_QUERY, capabilityGroupResultsList);
		returnHash.put(ConstantsTAP.MISSION_OUTCOME_QUERY, missionOutcomeResultsList);
		returnHash.put(ConstantsTAP.CONOPS_SOURCE_QUERY, conopsSourceResultsList);
		returnHash.put(ConstantsTAP.PARTICIPANT_QUERY, participantResultsList);
		returnHash.put(ConstantsTAP.TASK_COUNT_QUERY, taskCountResultsList);
		returnHash.put(ConstantsTAP.BP_COUNT_QUERY, bpCountResultsList);
		returnHash.put(ConstantsTAP.BR_COUNT_QUERY, brCountResultsList);
		returnHash.put(ConstantsTAP.BS_COUNT_QUERY, bsCountResultsList);
		returnHash.put(ConstantsTAP.TR_COUNT_QUERY, trCountResultsList);	
		returnHash.put(ConstantsTAP.TS_COUNT_QUERY, tsCountResultsList);
		returnHash.put(ConstantsTAP.DATA_COUNT_QUERY, dataCountResultsList);
		returnHash.put(ConstantsTAP.BLU_COUNT_QUERY, bluCountResultsList);
		returnHash.put(ConstantsTAP.FUNCTIONAL_GAP_COUNT_QUERY, functionalGapCountResultsList);
		returnHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_PROVIDE_DATA_COUNT_QUERY, capProvideSystemProvideCountResultsList);
		returnHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_CONSUME_DATA_COUNT_QUERY, capProvideSystemConsumeCountResultsList);
		returnHash.put(ConstantsTAP.CAP_CONSUME_SYSTEM_PROVIDE_DATA_COUNT_QUERY, capConsumeSystemProvideCountResultsList);


	
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
			String criteria ="Total Score";
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

		returnHash.put(ConstantsTAP.DATA_OBJECT_QUERY, dataObjectResultsList);
		masterHash.put(ConstantsTAP.DATA_OBJECT_QUERY, dataObjectResultsList);
		
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
		masterHash.put(ConstantsTAP.BLU_QUERY, bluObjectResultsList);
		
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
		masterHash.put(ConstantsTAP.FUNCTIONAL_GAP_QUERY, functionalGapResultsList);
		
		return returnHash;
	}

	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processTaskandBPQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	
		
		//Task
		String taskQuery = "SELECT DISTINCT ?Task  WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}}";		
		taskQuery = taskQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//BP
		String bpQuery = "SELECT DISTINCT ?BusinessProcess WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?Capability ?Consists ?Task.} {?Task ?Needs ?BusinessProcess}}";		
		bpQuery = bpQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> taskResultsList = runQuery(HRCoreEngine, taskQuery);
		ArrayList<Object> bpResultsList = runQuery(HRCoreEngine, bpQuery);

		returnHash.put(ConstantsTAP.TASK_QUERY, taskResultsList);
		returnHash.put(ConstantsTAP.BP_QUERY, bpResultsList);

		masterHash.put(ConstantsTAP.TASK_QUERY, taskResultsList);
		masterHash.put(ConstantsTAP.BP_QUERY, bpResultsList);
		return returnHash;
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processRequirementsAndStandardsQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	
		
		
		//BR Count
		String brQuery = "SELECT DISTINCT ?BusinessRule WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?BusinessRule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessRule>; }{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>; }{?Capability ?Consists ?Task.}{?Task ?Supports ?BusinessRule.} }";		
		brQuery = brQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//BS Count
		String bsQuery = "SELECT DISTINCT ?BusinessStandard WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?BusinessStandard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessStandard>; }{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>; }{?Capability ?Consists ?Task.}{?Task ?Supports ?BusinessStandard.} }";		
		bsQuery = bsQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		//TS Count
		String tsQuery = "SELECT DISTINCT ?TechRequirement (COALESCE(?TechStandard, 'None') AS ?Techstandard) ?TechCategory ?TechSubCategory WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>; }{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task> ; }{?Capability ?Consists ?Task.}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>; }{?Attribute  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute>; }{?Task ?Has ?Attribute.}  {?Satisfies <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Satisfies>; }{?TechRequirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechRequirement>}  OPTIONAL { {?TechStandard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechStandardIdentifier>; } {?TechRequirement_Has_TechStandard <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?TechRequirement ?TechRequirement_Has_TechStandard ?TechStandard} } {?TechSubCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechSubCategory>}{?TechSubCategory_Has_TechRequirement <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?TechSubCategory ?TechSubCategory_Has_TechRequirement ?TechRequirement}{?TechCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TechCategory>}{?TechCategory_Has_TechSubCategory <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?TechCategory ?TechCategory_Has_TechSubCategory ?TechSubCategory}{?TechRequirement ?Satisfies ?Attribute.}} ORDER BY ?TechCategory ?TechSubCategory ?TechRequirement";		
		tsQuery = tsQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);

				
		ArrayList<Object> brResultsList = runQuery(HRCoreEngine, brQuery);
		ArrayList<Object> bsResultsList = runQuery(HRCoreEngine, bsQuery);
		ArrayList<Object> tsResultsList = runQuery(HRCoreEngine, tsQuery);

		returnHash.put(ConstantsTAP.BR_QUERY, brResultsList);
		returnHash.put(ConstantsTAP.BS_QUERY, bsResultsList);
		returnHash.put(ConstantsTAP.TS_QUERY, tsResultsList);
		
		masterHash.put(ConstantsTAP.BR_QUERY, brResultsList);
		masterHash.put(ConstantsTAP.BS_QUERY, bsResultsList);
		masterHash.put(ConstantsTAP.TS_QUERY, tsResultsList);

		return returnHash;
		
		
		
	}
	
	/**
	 * Contains all the queries required to return the necessary data for the capability fact sheet reports
	 * @param capabilityName	String containing the capability name to run the queries on
	 * @return returnHash	Hashtable containing the results for all the queries
	 */
	public Hashtable processSystemQueries(String capabilityName) {
		
		Hashtable<String, Object> returnHash = new Hashtable<String, Object>();	

		String capProvideSysProvideQuery = "SELECT DISTINCT ?System ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task ?Needs ?Data.}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) }";		
		capProvideSysProvideQuery = capProvideSysProvideQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		String capProvideSysConsumeQuery = "SELECT DISTINCT ?System ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}OPTIONAL{{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task ?Needs ?Data.}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}FILTER(!BOUND(?icd2)) }";		
		capProvideSysConsumeQuery = capProvideSysConsumeQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
		
		String capConsumeSysProvideQuery = "SELECT DISTINCT ?System ?Data WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Access_a_Healthy_and_Fit_Force> AS ?Capability ){?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.}{?Task ?Needs ?Data.} {?System <http://semoss.org/ontologies/Relation/Provide> ?Data ;}{?icd <http://semoss.org/ontologies/Relation/Consume> ?System ;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data ;}}";		
		capConsumeSysProvideQuery = capConsumeSysProvideQuery.replaceAll("Access_a_Healthy_and_Fit_Force",capabilityName);
				
		ArrayList<Object> capProvideSysProvideResultsList = runQuery(HRCoreEngine, capProvideSysProvideQuery);
		ArrayList<Object> capProvideSysConsumeResultsList = runQuery(HRCoreEngine, capProvideSysConsumeQuery);
		ArrayList<Object> capConsumeSysProvideResultsList = runQuery(HRCoreEngine, capConsumeSysProvideQuery);

		returnHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_PROVIDE_DATA_QUERY, capProvideSysProvideResultsList);
		returnHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_CONSUME_DATA_QUERY, capProvideSysConsumeResultsList);
		returnHash.put(ConstantsTAP.CAP_CONSUME_SYSTEM_PROVIDE_DATA_QUERY, capConsumeSysProvideResultsList);
		
		masterHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_PROVIDE_DATA_QUERY, capProvideSysProvideResultsList);
		masterHash.put(ConstantsTAP.CAP_PROVIDE_SYSTEM_CONSUME_DATA_QUERY, capProvideSysConsumeResultsList);
		masterHash.put(ConstantsTAP.CAP_CONSUME_SYSTEM_PROVIDE_DATA_QUERY, capConsumeSysProvideResultsList);
	
	
		return returnHash;
	}
}