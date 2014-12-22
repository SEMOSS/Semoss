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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Procedure to evaluate BLU's and Data Objects from a system perspective to see which business processes and capabilities a system supports.
 */
public class SysBPCapInsertProcessor extends AggregationHelper {
	
	private static final Logger LOGGER = LogManager.getLogger(SysBPCapInsertProcessor.class.getName());
	private IEngine coreDB;
	
	public final String DATAC = "Data";
	public final String BLU = "BLU";
	
	private double dataObjectThresholdValue = 0.0;
	private double bluThresholdValue = 0.0;
	private String logicType = "AND";
	HashMap<String, HashMap<String, HashMap<String, Double>>> storageHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	
	private final String hrCoreBaseURI = "http://health.mil/ontologies/Relation/";
	
	public String errorMessage = "";
		
	private final String BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
	private final String BUSINESS_PROCESSES_BLU_QUERY = "SELECT DISTINCT ?BusinessProcess ?BLU WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	private final String CAPABILITY_DATA_QUERY = "SELECT DISTINCT ?Capability ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')('M')}";
	private final String CAPABILITY_BLU_QUERY = "SELECT DISTINCT ?Capability ?BLU WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	private final String SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} } BINDINGS ?CRM {('C')('M')}";
	private final String SYSTEM_BLU_QUERY = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?provide ?BLU.} }";
	private final String DELETE_NEW_RELATIONS_QUERY = "SELECT ?System ?relation ?o ?allInferredRelationships WHERE { {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> 'yes'} MINUS {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> 'yes' } {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?BP} {?relation ?allInferredRelationships ?o}}";
	private final String DELETE_NEW_PROPERTIES_QUERY = "SELECT ?relation ?pred ?Calculated WHERE {BIND(<http://semoss.org/ontologies/Relation/Contains/Calculated> AS ?pred) {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> ?Reported}  {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?o}} ";
	
	public String getErrorMessage() {
		return this.errorMessage;
	}

	public SysBPCapInsertProcessor(Double dataObjectThresholdValue, Double bluThresholdValue, String logicType) {
		this.dataObjectThresholdValue = dataObjectThresholdValue;
		this.bluThresholdValue = bluThresholdValue;
		this.logicType = logicType;
	}
	
	public void runDeleteQueries() {
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		String[] vars;
		
		sjsw = Utility.processQuery(coreDB, DELETE_NEW_RELATIONS_QUERY);
		vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{	
			SesameJenaSelectStatement sjss = sjsw.next();
			String sys = sjss.getRawVar(vars[0]).toString();
			String rel = sjss.getRawVar(vars[1]).toString();
			String obj = sjss.getRawVar(vars[2]).toString();
			String inf = sjss.getRawVar(vars[3]).toString();
			
			addToDeleteHash(new Object[]{sys, rel, obj});
			addToDeleteHash(new Object[]{rel, inf, obj});
		}
		
		sjsw = Utility.processQuery(coreDB, DELETE_NEW_PROPERTIES_QUERY);
		vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{	
			SesameJenaSelectStatement sjss = sjsw.next();
			String rel = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String calc = sjss.getRawVar(vars[2]).toString();
			
			addToDeleteHash(new Object[]{rel, pred, calc});
		}
		
		deleteData(coreDB, removeDataHash);
	}
	
	public boolean runCoreInsert()	{
		boolean success = true;		
		LOGGER.info("Data Object Threshold Value = " + dataObjectThresholdValue*100 + "%");
		LOGGER.info("Business Logic Unit Threshold Value = " + bluThresholdValue*100 + "%");
		LOGGER.info("Core DB is: " + coreDB.getEngineName());			
//1.  QUERY AND COLLECT THE DATA (Raw URIs)	
		HashMap<String, Set<String>> bpDataHash = getQueryResultHash(coreDB, BUSINESS_PROCESSES_DATA_QUERY);
		HashMap<String, Set<String>> bpBLUHash = getQueryResultHash(coreDB, BUSINESS_PROCESSES_BLU_QUERY);
		HashMap<String, Set<String>> systemDataHash = getQueryResultHash(coreDB, SYSTEM_DATA_QUERY);
		HashMap<String, Set<String>> systemBLUHash = getQueryResultHash(coreDB, SYSTEM_BLU_QUERY);				
		HashMap<String, Set<String>> capDataHash = getQueryResultHash(coreDB, CAPABILITY_DATA_QUERY);
		HashMap<String, Set<String>> capBLUHash = getQueryResultHash(coreDB, CAPABILITY_BLU_QUERY);		
			if (!(coreDB.getEngineName().equals("HR_Core"))) {
				this.errorMessage = "Select the HR_Core database.";
				return false;
			}				
			if (bpDataHash.isEmpty() || bpBLUHash.isEmpty() || systemDataHash.isEmpty() || systemBLUHash.isEmpty() || capDataHash.isEmpty() || capBLUHash.isEmpty()) {
				this.errorMessage = "One or more of the queries returned no results.";
				return false;
			}
//2.  Processing and Analysis
		allRelations.clear();
		dataHash.clear();
	//BP
		insertRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash);
	//Capabilities
		insertRelations(capDataHash, capBLUHash, systemDataHash, systemBLUHash);
//3.  Insert new relationships (Full URIs)	
	    processData(coreDB, dataHash);
	    for (String obj : allRelations.keySet()) {
	    	for (String sub : allRelations.get(obj)) {
	    	   processNewRelationshipsAtInstanceLevel(coreDB, sub, obj);
	    	}
	    }
	 	((BigDataEngine) coreDB).infer();
	 	
		return success;		
	}
	
	public void insertRelations(HashMap<String, Set<String>> bpDataHash, HashMap<String, Set<String>> bpBLUHash, HashMap<String, Set<String>> systemDataHash, HashMap<String, Set<String>> systemBLUHash){
		processRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash, true);
	}
	
	private void genRelationsForStorage(HashMap<String, Set<String>> bpDataHash, HashMap<String, Set<String>> bpBLUHash, HashMap<String, Set<String>> systemDataHash, HashMap<String, Set<String>> systemBLUHash){
		processRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash, false);
	}
	
	public void genStorageInformation(IEngine db, String infoType) {
		HashMap<String, Set<String>> systemDataHash = getQueryResultHash(db, SYSTEM_DATA_QUERY);
		HashMap<String, Set<String>> systemBLUHash = getQueryResultHash(db, SYSTEM_BLU_QUERY);	
		if (infoType.equals("BusinessProcess")) {
			HashMap<String, Set<String>> bpDataHash = getQueryResultHash(db, BUSINESS_PROCESSES_DATA_QUERY);
			HashMap<String, Set<String>> bpBLUHash = getQueryResultHash(db, BUSINESS_PROCESSES_BLU_QUERY);
			genRelationsForStorage(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash);
		}
		else if (infoType.equals("Capability")) {		
			HashMap<String, Set<String>> capDataHash = getQueryResultHash(db, CAPABILITY_DATA_QUERY);
			HashMap<String, Set<String>> capBLUHash = getQueryResultHash(db, CAPABILITY_BLU_QUERY);
			genRelationsForStorage(capDataHash, capBLUHash, systemDataHash, systemBLUHash);
		}
	}
	
	public HashMap<String, Set<String>> getQueryResultHash(IEngine db, String query) {
		HashMap<String, Set<String>> queryDataHash = new HashMap<String, Set<String>>();
		SesameJenaSelectWrapper queryDataWrapper = Utility.processQuery(db, query);
		queryDataHash = hashTableResultProcessor(queryDataWrapper);
		return queryDataHash;
	}
	
	public HashMap<String, Set<String>> hashTableResultProcessor(SesameJenaSelectWrapper sjsw) {
		HashMap<String, Set<String>> aggregatedData = new HashMap<String, Set<String>>();
		String[] vars = sjsw.getVariables();
		while (sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();			
			String sub = sjss.getRawVar(vars[0]).toString();
			Set<String> pred = new HashSet<String>();
			pred.add(sjss.getRawVar(vars[1]).toString());
			if (!aggregatedData.containsKey(sub))
				{aggregatedData.put(sub, pred);}
			else {aggregatedData.get(sub).add(sjss.getRawVar(vars[1]).toString());}				
		}						
		return aggregatedData;
	}
	
	private void processRelations(HashMap<String, Set<String>> bpDataHash, HashMap<String, Set<String>> bpBLUHash, HashMap<String, Set<String>> systemDataHash, HashMap<String, Set<String>> systemBLUHash, boolean insert) {	
	//for storage
		HashMap<String, HashMap<String, Double>> dataSubHash = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> bluSubHash = new HashMap<String, HashMap<String, Double>>();
	//populate the System, BP sets with the appropriate data from the query result Hashtables. (Raw URIs)		
		Set<String> overallBPSet = new HashSet<String>();
		overallBPSet.addAll(bpDataHash.keySet());
		overallBPSet.addAll(bpBLUHash.keySet());		
		Set<String> overallSystemSet = new HashSet<String>();
		overallSystemSet.addAll(systemDataHash.keySet());
		overallSystemSet.addAll(systemBLUHash.keySet());                                                                                                                                                                                                                                                                                                                                                                                                                            
		
		for (String bp : overallBPSet) {			
		//Process Query Results (BP Specific)
			Set<String> bpSpecificDataSet = new HashSet<String>();			
			if (!(bpDataHash.get(bp) == null)) {
				bpSpecificDataSet.addAll(bpDataHash.get(bp));}
			
			Set<String> bpSpecificBLUSet = new HashSet<String>();			
			if (!(bpBLUHash.get(bp) == null)) {
				bpSpecificBLUSet.addAll(bpBLUHash.get(bp));}	
			
			HashMap<String, Double> dataScoreHash = new HashMap<String, Double>();
			HashMap<String, Double> bluScoreHash = new HashMap<String, Double>();
				
			for (String sys : overallSystemSet) {
				int systemSpecificDataCount = 0, systemSpecificBLUCount = 0;				
			//Figure out what Data Objects a system creates or reads and what BLUs that system provides
				Set<String> systemSpecificDataSet = systemDataHash.get(sys);
				Set<String> systemSpecificBLUSet = systemBLUHash.get(sys);
			
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
			
				if(insert){
				//Based on the logic type, insert new relationships	
					if (logicType.equals("AND")) {		
						if ( (systemSpecificDataCount >= ( bpSpecificDataSet.size()*dataObjectThresholdValue )) & (systemSpecificBLUCount >= ( bpSpecificBLUSet.size()*bluThresholdValue )) ) {	
							systemSupportsBPRelationProcessing(sys, bp);
						}
					}
					else if (logicType.equals("OR")) {
						if ( (systemSpecificDataCount >= ( bpSpecificDataSet.size()*dataObjectThresholdValue )) || (systemSpecificBLUCount >= ( bpSpecificBLUSet.size()*bluThresholdValue )) ) {
							systemSupportsBPRelationProcessing(sys, bp);
						}
					}
				}
				else {
					double sysDataScore = 0.0, sysBLUScore = 0.0;						
					if (bpSpecificDataSet.size()!=0)
						sysDataScore = (double) systemSpecificDataCount/bpSpecificDataSet.size();
					if (bpSpecificBLUSet.size()!=0)
						sysBLUScore = (double) systemSpecificBLUCount/bpSpecificBLUSet.size();
					boolean test = ((sysDataScore > 0) || (sysBLUScore > 0));
					String sysInstance = Utility.getInstanceName(sys);
					if (test) {
						dataScoreHash.put(sysInstance, sysDataScore);
						bluScoreHash.put(sysInstance, sysBLUScore);
					}	
				}
			}
			String bpInstance = Utility.getInstanceName(bp);
			dataSubHash.put(bpInstance, dataScoreHash);
			bluSubHash.put(bpInstance, bluScoreHash);
		}
		
		if (insert) {
			addToDataHash(new Object[]{semossPropertyBaseURI + "Calculated", RDF.TYPE.toString(), semossRelationBaseURI + "Contains"});
			//logger.info("*****SubProp URI: "+ semossPropertyBaseURI + "Calculated" + " typeURI : " + RDF.TYPE.toString() + " propbase: " + semossRelationBaseURI + "Contains");		
		}
		else {
			storageHash.put(DATAC, dataSubHash);
			storageHash.put(BLU, bluSubHash);
		}
	}
	
	/**
	 * Add the System-BP relation to the local Hashtables to prepare for Insert
	 * @param sys
	 * @param bp
	 */
	public void systemSupportsBPRelationProcessing(String sys, String bp) {					
		String pred = hrCoreBaseURI + "Supports";
		pred = pred + "/" + getTextAfterFinalDelimeter(sys, "/") +":" + getTextAfterFinalDelimeter(bp, "/");
		addToDataHash(new Object[]{sys, pred, bp});
		addToDataHash(new Object[]{pred, semossPropertyBaseURI + "Calculated", "yes"});
		//logger.info("*****Prop URI: " + pred + ", predURI: " + semossPropertyBaseURI + "Calculated" + ", value: " + "yes");
		addToAllRelationships(pred);					
		LOGGER.info("System: " + sys + ", BP: " + bp + ", Pred: " + pred);
	}

	
	public void setInsertCoreDB(String insertEngine) {
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(insertEngine);
	}
}
