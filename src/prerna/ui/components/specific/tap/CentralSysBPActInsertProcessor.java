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

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Procedure to evaluate BLU's and Data Objects from a system perspective to see which business processes a system supports.
 */
public class CentralSysBPActInsertProcessor {
	
	Logger logger = Logger.getLogger(getClass());
	private IEngine coreDB;
	
	private double dataObjectThresholdValue = 0.0;
	private double bluThresholdValue = 0.0;
	
	//TODO get the URI from the data itself?
	private String hrCoreBaseURI = "http://health.mil/ontologies/Relation/";

	private Hashtable<String, Hashtable<String, Object>> dataHash = new Hashtable<String, Hashtable<String, Object>>();
	private Hashtable<String, Set<String>> newRelationships = new Hashtable<String, Set<String>>();

	public String errorMessage = "";
		
	/**Data w/ CRM*/
	//private String TAP_CORE_BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data ?CRM WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}}";
	private String TAP_CORE_BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
	private String TAP_CORE_BUSINESS_PROCESSES_BLU_QUERY = "SELECT DISTINCT ?BusinessProcess ?BLU WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	private String TAP_CORE_CAPABILITY_DATA_QUERY = "SELECT DISTINCT ?Capability ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
	private String TAP_CORE_CAPABILITY_BLU_QUERY = "SELECT DISTINCT ?Capability ?BLU WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	private String TAP_CORE_SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} }";
	private String TAP_CORE_SYSTEM_BLU_QUERY = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?provide ?BLU.} }";
	//private String DELETE_NEW_RELATIONS_QUERY = "DELETE {?System ?relation ?BP. ?relation ?x ?y. ?System ?allInferredRelationships ?BP} WHERE { {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> 'yes'} MINUS {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> 'yes' } {?relation ?x ?y} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?BP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?System ?relation ?BP} {?System ?allInferredRelationships ?BP}}";
	//private String DELETE_NEW_PROPERTIES_QUERY = "DELETE {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} WHERE {{?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> ?Reported}  {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?BP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?System ?relation ?BP}} ";
	private String DELETE_NEW_RELATIONS_QUERY = "DELETE {?System ?relation ?o. ?relation ?x ?y. ?System ?allInferredRelationships ?o} WHERE { {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> 'yes'} MINUS {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> 'yes' } {?relation ?x ?y} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?BP} {?System ?allInferredRelationships ?o}}";
	private String DELETE_NEW_PROPERTIES_QUERY = "DELETE {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} WHERE {{?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> ?Reported}  {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?o}} ";
	
	public String getErrorMessage() {
		return this.errorMessage;
	}

	public CentralSysBPActInsertProcessor(Double dataObjectThresholdValue, Double bluThresholdValue) {
		this.dataObjectThresholdValue = dataObjectThresholdValue;
		this.bluThresholdValue = bluThresholdValue;
	}
	
	public void runDeleteQueries() {
		UpdateProcessor upProc = new UpdateProcessor();
		upProc.setQuery(DELETE_NEW_RELATIONS_QUERY);
		logger.info("Deleting Calculated Relationships...");
		upProc.processQuery();
		upProc.setQuery(DELETE_NEW_PROPERTIES_QUERY);
		logger.info("Deleting Calculated Properties...");
		upProc.processQuery();
	}
	
	public boolean runCoreInsert()	{
		boolean success = true;
		
		AggregationHelper aggregationHelper = new AggregationHelper();
		
		logger.info("Data Object Threshold Value = " + dataObjectThresholdValue*100 + "%");
		logger.info("Business Logic Unit Threshold Value = " + bluThresholdValue*100 + "%");
		logger.info("Core DB is: " + coreDB);		
		
//1.  QUERY AND COLLECT THE DATA (Raw URIs)	
	//1.1  Collect bp==>Data (CRM) and bp==>BLU 		
		SesameJenaSelectWrapper bpDataListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_BUSINESS_PROCESSES_DATA_QUERY);
		SesameJenaSelectWrapper bpBLUListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_BUSINESS_PROCESSES_BLU_QUERY);
	//1.2  Collect Sys==>Data (CRM) and Sys==>BLU 	
		SesameJenaSelectWrapper systemDataWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_SYSTEM_DATA_QUERY);
		SesameJenaSelectWrapper systemBLUWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_SYSTEM_BLU_QUERY);
		
		SesameJenaSelectWrapper capDataListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_CAPABILITY_DATA_QUERY);
		SesameJenaSelectWrapper capBLUListWrapper = aggregationHelper.processQuery(coreDB, TAP_CORE_CAPABILITY_BLU_QUERY);
		
//2.  PROCESS THE DATA AND PERFORM ANALYSIS	
	//Processing
		Hashtable bpDataHash = aggregationHelper.hashTableResultProcessor(bpDataListWrapper);
		Hashtable bpBLUHash = aggregationHelper.hashTableResultProcessor(bpBLUListWrapper);
		Hashtable systemDataHash = aggregationHelper.hashTableResultProcessor(systemDataWrapper);
		Hashtable systemBLUHash = aggregationHelper.hashTableResultProcessor(systemBLUWrapper);				
		Hashtable capDataHash = aggregationHelper.hashTableResultProcessor(capDataListWrapper);
		Hashtable capBLUHash = aggregationHelper.hashTableResultProcessor(capBLUListWrapper);
		
		if (bpDataHash.isEmpty() || bpBLUHash.isEmpty() || systemDataHash.isEmpty() || systemBLUHash.isEmpty() || capDataHash.isEmpty() || capBLUHash.isEmpty()) {
			this.errorMessage = "One or more of the queries returned no results.";
			return false;
		}
		
	//Analysis
		processRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash);
		processRelations(capDataHash, capBLUHash, systemDataHash, systemBLUHash);

//3.  Insert new relationships (Full URIs)			
		aggregationHelper.processData(coreDB, dataHash);
		aggregationHelper.processNewRelationships(coreDB, newRelationships);
		((BigDataEngine) coreDB).infer();
			
		return success;		
	}
	
	private void processRelations(Hashtable bpDataHash, Hashtable bpBLUHash, Hashtable systemDataHash, Hashtable systemBLUHash) {		
		AggregationHelper aggregationHelper = new AggregationHelper();			
	//populate the System, BP sets with the appropriate data from the query result hashtables. (Raw URIs)
		
		Set<String> overallBPSet = new HashSet<String>();
		overallBPSet.addAll(bpDataHash.keySet());
		overallBPSet.addAll(bpBLUHash.keySet());
		
		Set<String> overallSystemSet = new HashSet<String>();
		overallSystemSet.addAll(systemDataHash.keySet());
		overallSystemSet.addAll(systemBLUHash.keySet());
		
		for (String bp : overallBPSet) {			
		//Process Query Results (BP Specific)
			/**For Debugging*/
			/*if (bp.equals("http://health.mil/ontologies/Concept/BusinessProcess/Make_Query")) {
				System.out.println("pause");
			}*/
			
			Set<String> bpSpecificDataSet = new HashSet<String>();			
			//boolean test1 = (!(bpDataHash.get(bp) == null));
			if (!(bpDataHash.get(bp) == null)) {
				bpSpecificDataSet.addAll((Set<String>) bpDataHash.get(bp));}
			
			Set<String> bpSpecificBLUSet = new HashSet<String>();			
			//boolean test = (!(bpBLUHash.get(bp) == null));
			if (!(bpBLUHash.get(bp) == null)) {
				bpSpecificBLUSet.addAll((Set<String>) bpBLUHash.get(bp));}	
				
				for (String sys : overallSystemSet) {
					int systemSpecificDataCount = 0, systemSpecificBLUCount = 0;				
				//Figure out what Data Objects a system creates or reads and what BLUs that system provides
					Set<String> systemSpecificDataSet = (Set<String>) systemDataHash.get(sys);
					Set<String> systemSpecificBLUSet = (Set<String>) systemBLUHash.get(sys);
					
					/**For Debugging*/
					/*if (sys.equals("http://health.mil/ontologies/Concept/System/PEPR") & bp.equals("http://health.mil/ontologies/Concept/BusinessProcess/Make_Query")) {
						System.out.println("pause");
					}*/
					
				//check to see if system created data objects ('d') is needed by the Business Process 'bp'
					if(!(systemSpecificDataSet == null)) {
						for (String dataObj : bpSpecificDataSet) {					
							if (systemSpecificDataSet.contains(dataObj)) {systemSpecificDataCount++;}
						}
					}
				//check to see if system provided BLUs ('b') is needed by the Business Process 'bp'	
					if (!(systemSpecificBLUSet == null)) {
						for (String blu : bpSpecificBLUSet) {					
							if (systemSpecificBLUSet.contains(blu)) {systemSpecificBLUCount++;}
						} 
					}	
					
				//Decide if System 's' Supports BusinessProcess	
					//boolean finalTest = ( (systemSpecificDataCount >= ( bpSpecificDataSet.size()*dataObjectThresholdValue )) & (systemSpecificBLUCount >= ( bpSpecificBLUSet.size()*bluThresholdValue )) );
					if ( (systemSpecificDataCount >= ( bpSpecificDataSet.size()*dataObjectThresholdValue )) & (systemSpecificBLUCount >= ( bpSpecificBLUSet.size()*bluThresholdValue )) ) {						
					//Add the System-BP relation to the local Hashtables to prepare for Insert						
						String pred = hrCoreBaseURI + "Supports";
						pred = pred + "/" + aggregationHelper.getTextAfterFinalDelimeter(sys, "/") +":" + aggregationHelper.getTextAfterFinalDelimeter(bp, "/");
						dataHash = aggregationHelper.addToHash(dataHash, new Object[]{sys, pred, bp});
						//dataHash = aggregationHelper.addToHash(dataHash, new Object[]{pred, aggregationHelper.getSemossPropertyBaseURI() + "Calculated", "\"true\"^^<xsd:boolean>"});
						//logger.info("*****Prop URI: " + pred + ", predURI: " + aggregationHelper.getSemossPropertyBaseURI() + "Calculated" + ", value: " + "\"true\"^^<xsd:boolean>");
						dataHash = aggregationHelper.addToHash(dataHash, new Object[]{pred, aggregationHelper.getSemossPropertyBaseURI() + "Calculated", "yes"});
						logger.info("*****Prop URI: " + pred + ", predURI: " + aggregationHelper.getSemossPropertyBaseURI() + "Calculated" + ", value: " + "yes");
						newRelationships = aggregationHelper.addNewRelationships(newRelationships, pred);						
						logger.info("System: " + sys + ", BP: " + bp + ", Pred: " + pred);
					}
				}
		}
		dataHash = aggregationHelper.addToHash(dataHash, new Object[]{aggregationHelper.getSemossPropertyBaseURI()+"Calculated", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", aggregationHelper.getSemossRelationBaseURI() + "Contains"});
		logger.info("*****SubProp URI: "+ aggregationHelper.getSemossPropertyBaseURI()+"Calculated" + " typeURI : "+"http://www.w3.org/1999/02/22-rdf-syntax-ns#type" +" propbase: " + aggregationHelper.getSemossRelationBaseURI() + "Contains");
	}

	
	public void setInsertCoreDB(String insertEngine) {
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(insertEngine);
	}	
}
