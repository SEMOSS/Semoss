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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.apache.log4j.Logger;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class CentralSysBPActInsertProcessor {
	
	Logger logger = Logger.getLogger(getClass());
	private IEngine coreDB;
	private String hrCoreEngine = "HR_Core";
	
	private double dataObjectThresholdValue = 0.0;
	private double bluThresholdValue = 0.0;

	private Hashtable<String, Hashtable<String, Object>> dataHash = new Hashtable<String, Hashtable<String, Object>>();
	private Hashtable<String, Set<String>> newRelationships = new Hashtable<String, Set<String>>();

	public String errorMessage = "";
	
	//private String TAP_CORE_CENTRAL_SYSTEM_LIST_QUERY = "SELECT DISTINCT ?System WHERE { BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System} }";
	//TODO Ask Bill or George which to use
	//private String TAP_CORE_CENTRAL_SYSTEM_LIST_QUERY = "SELECT DISTINCT ?System WHERE { BIND(<http://health.mil/ontologies/Concept/SystemOwner/Central> AS ?SystemOwner) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  {?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner} }";
	
	//private String TAP_CORE_BUSINESS_PROCESSES_QUERY = "SELECT DISTINCT ?BusinessProcess WHERE { {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} }";
	private String TAP_CORE_BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data ?CRM WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}}";
	private String TAP_CORE_BUSINESS_PROCESSES_BLU_QUERY = "SELECT DISTINCT ?BusinessProcess ?BLU WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	//private String TAP_CORE_ACTIVITY_DATA_QUERY = "SELECT DISTINCT ?Activity ?Data WHERE {{?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Activity ?Needs ?Data.} }";
	//private String TAP_CORE_ACTIVITY_BLU_QUERY = "SELECT DISTINCT ?Activity ?BLU WHERE { {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Activity ?Needs ?BLU.} }";	
	private String TAP_CORE_SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System}}";
	private String TAP_CORE_SYSTEM_BLU_QUERY = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?provide ?BLU.} BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System} }";
	
	public String getErrorMessage() {
		return this.errorMessage;
	}

	public CentralSysBPActInsertProcessor(Double dataObjectThresholdValue, Double bluThresholdValue) {
		this.dataObjectThresholdValue = dataObjectThresholdValue;
		this.bluThresholdValue = bluThresholdValue;
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
	}
	
	public boolean runCoreInsert()	{
		boolean success = true;
		
		AggregationHelper aggregationHelper = new AggregationHelper();
		aggregationHelper.setCoreDB(coreDB);
		
		logger.info("Data Object Threshold Value = " + dataObjectThresholdValue*100 + "%");
		logger.info("Business Logic Unit Threshold Value = " + bluThresholdValue*100 + "%");
		logger.info("Core DB is: " + coreDB);		
		
//1.  QUERY AND COLLECT THE DATA (Raw URIs)
	//1.1  Collect Sys==>Data (CRM) and Sys==>BLU 		
		SesameJenaSelectWrapper bpDataListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_BUSINESS_PROCESSES_DATA_QUERY);
		SesameJenaSelectWrapper bpBLUListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_BUSINESS_PROCESSES_BLU_QUERY);
		SesameJenaSelectWrapper systemDataWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_SYSTEM_DATA_QUERY);
		SesameJenaSelectWrapper systemBLUWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_SYSTEM_BLU_QUERY);
		
//2.  PROCESS THE DATA AND PERFORM ANALYSIS	
	//Processing
		ArrayList<ArrayList<Object>> bpDataList = aggregationHelper.arrayListResultProcessor(bpDataListWrapper);
		ArrayList<ArrayList<Object>> bpBLUList = aggregationHelper.arrayListResultProcessor(bpBLUListWrapper);
		ArrayList<ArrayList<Object>> systemData = aggregationHelper.arrayListResultProcessor(systemDataWrapper);
		ArrayList<ArrayList<Object>> systemBLU = aggregationHelper.arrayListResultProcessor(systemBLUWrapper);		
	//Analysis
		processSystemToBP(bpDataList, bpBLUList, systemData, systemBLU);			

//3.  Insert new relationships (Full URIs)			
		aggregationHelper.processData(dataHash);
		aggregationHelper.processNewRelationships(newRelationships);
		
		if(!errorMessage.isEmpty()) {
			success = false;
		}
		
		return success;		
	}
	
	private void processSystemToBP(ArrayList<ArrayList<Object>> bpDataList, ArrayList<ArrayList<Object>> bpBLUList,
								   ArrayList<ArrayList<Object>> systemData, ArrayList<ArrayList<Object>> systemBLU) {
		
		AggregationHelper aggregationHelper = new AggregationHelper();
				
		HashSet<String> overallBPSet = new HashSet<String>();
		HashSet<String> overallSystemSet = new HashSet<String>();
		
	//populate the System, BP sets with the data from the query result array list. (Raw URIs)
		for (int i = 0; i < bpDataList.size(); i++) {
			ArrayList tempList = (ArrayList) bpDataList.get(i);
			overallBPSet.add(tempList.get(0).toString());		
		}
		for (int i = 0; i < bpBLUList.size(); i++) {
			ArrayList tempList = (ArrayList) bpBLUList.get(i);
			overallBPSet.add(tempList.get(0).toString());		
		}
		for (int i = 0; i < systemBLU.size(); i++) {
			ArrayList tempList = (ArrayList) systemBLU.get(i);
			overallSystemSet.add(tempList.get(0).toString());		
		}
		for (int i = 0; i < systemData.size(); i++) {
			ArrayList tempList = (ArrayList) systemData.get(i);
			overallSystemSet.add(tempList.get(0).toString());		
		}
			
	//System.out.println(overallBPSet.size());
		
		for (String bp : overallBPSet) {
			Hashtable<String, String> bpSpecificDataHash = new Hashtable<String, String>();
			ArrayList<String> bpSpecificBLUList = new ArrayList<String>();	
			
	//Process Query Results (BP Specific)
				for (int i = 0; i < bpDataList.size(); i++) {
					ArrayList tempList = (ArrayList) bpDataList.get(i);
					//System.out.println((tempList.get(0)).toString().contains(bp));
					if ((tempList.get(0)).toString().contains(bp)) { 
			//Prioritize C over R
						if((tempList.get(2).toString().contains("R"))) {
							if(!bpSpecificDataHash.containsKey(tempList.get(1))) {
								bpSpecificDataHash.put(tempList.get(1).toString(), tempList.get(2).toString());
							}
						}
						if((tempList.get(2).toString().contains("C"))) {
							bpSpecificDataHash.put(tempList.get(1).toString(), tempList.get(2).toString());
						}
						
					}
				}	
				for (int i = 0; i < bpBLUList.size(); i++) {
					ArrayList tempList = (ArrayList) bpBLUList.get(i);
					//System.out.println((tempList.get(0)).equals(bp));
					if ((tempList.get(0)).toString().contains(bp)) { 
						bpSpecificBLUList.add(tempList.get(1).toString());
					}
				}		
			
				for (String s : overallSystemSet) {
					int systemSpecificDataCount = 0, 
						systemSpecificBLUCount = 0;
				
	//Figure out what Data Objects a system creates or reads
					Hashtable<String, String> systemSpecificDataHash = new Hashtable<String, String>();
					for (int i = 0; i < systemData.size(); i++) {
						ArrayList tempList = (ArrayList) systemData.get(i);
						if (tempList.get(0).toString().contains(s) & (tempList.get(2).toString()).contains("R")) {
							if(!systemSpecificDataHash.containsKey(tempList.get(1))) {
								systemSpecificDataHash.put(tempList.get(1).toString(), tempList.get(2).toString());
							}							
						}
						if (tempList.get(0).toString().contains(s) & (tempList.get(2).toString()).contains("C")) { 	
							systemSpecificDataHash.put(tempList.get(1).toString(), tempList.get(2).toString());
						}
					}	
	//Figure out what BLUs a system provides
					ArrayList<String> systemSpecificBLUList = new ArrayList<String>();
					for (int i = 0; i < systemBLU.size(); i++) {
						ArrayList tempList = (ArrayList) systemBLU.get(i);
						if (tempList.get(0).toString().contains(s)) {
							systemSpecificBLUList.add(tempList.get(1).toString());
						}
					}		
					
	//check to see if system created data objects ('d') is needed by the Business Process 'bp' 			
					for (String d : systemSpecificDataHash.keySet()) {					
						if (bpSpecificDataHash.containsKey(d)) {
							if (systemSpecificDataHash.get(d).contains(bpSpecificDataHash.get(d)) || systemSpecificDataHash.get(d).contains("C") && bpSpecificDataHash.get(d).contains("R")) {
								systemSpecificDataCount++;
							}
						}
					}
	//check to see if system provided BLUs ('b') is needed by the Business Process 'bp'	
					for (String b : systemSpecificBLUList) {					
						if (bpSpecificBLUList.contains(b)) {
							systemSpecificBLUCount++;
						}
					} 
					
	//Decide if System 's' Supports BusinessProcess	
					if ( (systemSpecificDataCount > ( bpSpecificDataHash.keySet().size()*dataObjectThresholdValue ))
							& (systemSpecificBLUCount > ( bpSpecificBLUList.size()*bluThresholdValue )) ) {
						
						//Add the System-BP relation to the local Hashtables to prepare for Insert						
							String pred = aggregationHelper.getSemossRelationBaseURI() + "Supports";
							pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + aggregationHelper.getTextAfterFinalDelimeter(s, "/") +":" + aggregationHelper.getTextAfterFinalDelimeter(bp, "/");
							dataHash = aggregationHelper.addToHash(dataHash, new Object[]{s, pred, bp});
							newRelationships = aggregationHelper.addNewRelationships(newRelationships, pred);						
							logger.info("System: " + s + ", BP: " + bp + ", Pred: " + pred);
					}
				}				
		}
	}
}
