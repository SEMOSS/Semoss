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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

/**
 * Heat Map that shows how much a System Supports a Capability
 */
public class SysCapSimHeatMapSheet extends SimilarityHeatMapSheet {

	String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	String capabilityQuery = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}}";
	
	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public SysCapSimHeatMapSheet() {
		super();
		setComparisonObjectTypes("Capability", "System");	
	}	
	
	public void createData() {
		addPanel();
		SimilarityFunctions sdf = new SimilarityFunctions();
		AggregationHelper aggregationHelper = new AggregationHelper();		
		SysBPCapInsertProcessor processor = new SysBPCapInsertProcessor(0.0, 0.0, "AND");
		
		//get list of capabilities first
		updateProgressBar("10%...Getting all Capabilities for evaluation", 10);
		comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
		sdf.setComparisonObjectList(comparisonObjectList);
		//get list of systems
		updateProgressBar("20%...Getting all Systems for evaluation", 20);
		
		//query the Data
		//String BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
		//String BUSINESS_PROCESSES_BLU_QUERY = "SELECT DISTINCT ?BusinessProcess ?BLU WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
		//String CAPABILITY_SYSTEM_DATA_C_QUERY = "SELECT DISTINCT ?Capability (COALESCE(?System,'NA') AS ?Sys) ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} OPTIONAL {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?provide ?Data .}}} BINDINGS ?CRM {('C')}";
		//String CAPABILITY_SYSTEM_DATA_R_QUERY = "SELECT DISTINCT ?Capability (COALESCE(?System,'NA') AS ?Sys) ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} OPTIONAL {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?provide ?Data .}}} BINDINGS ?CRM {('R')}";
		//String CAPABILITY_SYSTEM_BLU_QUERY = "SELECT DISTINCT ?Capability (COALESCE(?System,'NA') AS ?Sys) ?BLU WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} OPTIONAL {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?System ?provide ?BLU.}}}";		
		
//1.  QUERY AND COLLECT THE DATA (Raw URIs)	
	//1.1  Collect bp==>Data (CRM) and bp==>BLU 		
		//SesameJenaSelectWrapper bpDataListWrapper = aggregationHelper.processQuery(coreDB, processor.BUSINESS_PROCESSES_DATA_QUERY);
		//SesameJenaSelectWrapper bpBLUListWrapper = aggregationHelper.processQuery(coreDB, processor.BUSINESS_PROCESSES_BLU_QUERY);
	//1.2  Collect Sys==>Data (CRM) and Sys==>BLU 	
		SesameJenaSelectWrapper systemDataWrapper = aggregationHelper.processQuery(coreDB, processor.SYSTEM_DATA_QUERY);
		SesameJenaSelectWrapper systemBLUWrapper = aggregationHelper.processQuery(coreDB, processor.SYSTEM_BLU_QUERY);
		
		SesameJenaSelectWrapper capDataListWrapper = aggregationHelper.processQuery(coreDB, processor.CAPABILITY_DATA_QUERY);
		SesameJenaSelectWrapper capBLUListWrapper = aggregationHelper.processQuery(coreDB, processor.CAPABILITY_BLU_QUERY);
		
//2.  PROCESS THE DATA AND PERFORM ANALYSIS	
	//Processing
		//Hashtable bpDataHash = aggregationHelper.hashTableResultProcessor(bpDataListWrapper);
		//Hashtable bpBLUHash = aggregationHelper.hashTableResultProcessor(bpBLUListWrapper);
		Hashtable systemDataHash = aggregationHelper.hashTableResultProcessor(systemDataWrapper);
		Hashtable systemBLUHash = aggregationHelper.hashTableResultProcessor(systemBLUWrapper);				
		Hashtable capDataHash = aggregationHelper.hashTableResultProcessor(capDataListWrapper);
		Hashtable capBLUHash = aggregationHelper.hashTableResultProcessor(capBLUListWrapper);
		
		processor.genRelationsForStorage(capDataHash, capBLUHash, systemDataHash, systemBLUHash);
		Hashtable resultHash = processor.storageHash;		
		
		updateProgressBar("50%...Evaluating Data Objects Created for a Capability", 50);
		Hashtable dataCScoreHash = (Hashtable) resultHash.get(processor.DATAC);
		logger.info("Finished Data Objects Created Processing.");
		dataCScoreHash = processHashForCharting(dataCScoreHash);		
		
		/*updateProgressBar("60%...Evaluating Data Objects Read for a Capability", 60);
		Hashtable dataRScoreHash = sdf.compareDifferentObjectParameterScore(hrCoreDB, CAPABILITY_SYSTEM_DATA_R_QUERY, SimilarityFunctions.VALUE);
		logger.info("Finished Data Objects Read Processing.");
		dataRScoreHash = processHashForCharting(dataRScoreHash);*/		
		
		updateProgressBar("70%...Evaluating Business Logic Provided for a Capability", 70);
		Hashtable bluScoreHash = (Hashtable) resultHash.get(processor.BLU);
		logger.info("Finished Business Logic Provided Processing.");
		bluScoreHash = processHashForCharting(bluScoreHash);		
		
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		
		//paramDataHash.put("Data_Objects_Created", dataCScoreHash);
		//paramDataHash.put("Data_Objects_Read", dataRScoreHash);
		//paramDataHash.put("Business_Logic_Provided", bluScoreHash);
		
		paramDataHash.put("Data_Objects_Created", dataCScoreHash);
		//paramDataHash.put("Data_Objects_Read", dataRScoreHash);
		paramDataHash.put("Business_Logic_Provided", bluScoreHash);
		
		allHash.put("title",  "Systems Support Capability");
		allHash.put("xAxisTitle", "Capability");
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}
}












