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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

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
	
	Logger logger = Logger.getLogger(getClass());
	private IEngine coreDB;
	
	public final String DATAC = "Data";
	public final String BLU = "BLU";
	
	private double dataObjectThresholdValue = 0.0;
	private double bluThresholdValue = 0.0;
	private String logicType = "AND";
	Hashtable<String, Hashtable<String, Hashtable<String, Double>>> storageHash = new Hashtable<String, Hashtable<String, Hashtable<String, Double>>>();
	
	private String hrCoreBaseURI = "http://health.mil/ontologies/Relation/";
	
	public String errorMessage = "";
		
	public String BUSINESS_PROCESSES_DATA_QUERY = "SELECT DISTINCT ?BusinessProcess ?Data WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
	public String BUSINESS_PROCESSES_BLU_QUERY = "SELECT DISTINCT ?BusinessProcess ?BLU WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Task ?Needs ?BusinessProcess} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	public String CAPABILITY_DATA_QUERY = "SELECT DISTINCT ?Capability ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Task ?Needs1 ?Data.} {?Needs1 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;} } BINDINGS ?CRM {('C')}";
	public String CAPABILITY_BLU_QUERY = "SELECT DISTINCT ?Capability ?BLU WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task} {?Needs1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Task ?Needs1 ?BLU.} }";
	public String SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} } BINDINGS ?CRM {('C')}";
	public String SYSTEM_BLU_QUERY = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?provide ?BLU.} }";
	private String DELETE_NEW_RELATIONS_QUERY = "SELECT ?System ?relation ?o ?allInferredRelationships WHERE { {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> 'yes'} MINUS {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> 'yes' } {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?BP} {?relation ?allInferredRelationships ?o}}";
	private String DELETE_NEW_PROPERTIES_QUERY = "SELECT ?relation ?pred ?Calculated WHERE {BIND(<http://semoss.org/ontologies/Relation/Contains/Calculated> AS ?pred) {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> ?Reported}  {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?o}} ";
	
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
		
		sjsw = processQuery(coreDB, DELETE_NEW_RELATIONS_QUERY);
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
		
		sjsw = processQuery(coreDB, DELETE_NEW_PROPERTIES_QUERY);
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
		logger.info("Data Object Threshold Value = " + dataObjectThresholdValue*100 + "%");
		logger.info("Business Logic Unit Threshold Value = " + bluThresholdValue*100 + "%");
		logger.info("Core DB is: " + coreDB.getEngineName());			
//1.  QUERY AND COLLECT THE DATA (Raw URIs)	
		Hashtable bpDataHash = getQueryResultHash(coreDB, BUSINESS_PROCESSES_DATA_QUERY);
		Hashtable bpBLUHash = getQueryResultHash(coreDB, BUSINESS_PROCESSES_BLU_QUERY);
		Hashtable systemDataHash = getQueryResultHash(coreDB, SYSTEM_DATA_QUERY);
		Hashtable systemBLUHash = getQueryResultHash(coreDB, SYSTEM_BLU_QUERY);				
		Hashtable capDataHash = getQueryResultHash(coreDB, CAPABILITY_DATA_QUERY);
		Hashtable capBLUHash = getQueryResultHash(coreDB, CAPABILITY_BLU_QUERY);		
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
	
	public void insertRelations(Hashtable bpDataHash, Hashtable bpBLUHash, Hashtable systemDataHash, Hashtable systemBLUHash){
		processRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash, true);
	}
	
	private void genRelationsForStorage(Hashtable bpDataHash, Hashtable bpBLUHash, Hashtable systemDataHash, Hashtable systemBLUHash){
		processRelations(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash, false);
	}
	
	public void genStorageInformation(IEngine db, String infoType) {
		Hashtable systemDataHash = getQueryResultHash(db, SYSTEM_DATA_QUERY);
		Hashtable systemBLUHash = getQueryResultHash(db, SYSTEM_BLU_QUERY);	
		if (infoType.equals("Capability")) {
			Hashtable bpDataHash = getQueryResultHash(db, BUSINESS_PROCESSES_DATA_QUERY);
			Hashtable bpBLUHash = getQueryResultHash(db, BUSINESS_PROCESSES_BLU_QUERY);
			genRelationsForStorage(bpDataHash, bpBLUHash, systemDataHash, systemBLUHash);
		}
		else if (infoType.equals("BusinessProcess")) {		
			Hashtable capDataHash = getQueryResultHash(db, CAPABILITY_DATA_QUERY);
			Hashtable capBLUHash = getQueryResultHash(db, CAPABILITY_BLU_QUERY);
			genRelationsForStorage(capDataHash, capBLUHash, systemDataHash, systemBLUHash);
		}
	}
	
	public Hashtable getQueryResultHash(IEngine db, String query) {
		Hashtable queryDataHash = new Hashtable();
		SesameJenaSelectWrapper queryDataWrapper = processQuery(db, query);
		queryDataHash = hashTableResultProcessor(queryDataWrapper);
		return queryDataHash;
	}
	
	public Hashtable hashTableResultProcessor(SesameJenaSelectWrapper sjsw) {
		Hashtable<String, Set<String>> aggregatedData = new Hashtable<String, Set<String>>();
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
	
	private void processRelations(Hashtable bpDataHash, Hashtable bpBLUHash, Hashtable systemDataHash, Hashtable systemBLUHash, boolean insert) {	
	//for storage
		Hashtable<String, Hashtable<String, Double>> dataSubHash = new Hashtable<String, Hashtable<String, Double>>();
		Hashtable<String, Hashtable<String, Double>> bluSubHash = new Hashtable<String, Hashtable<String, Double>>();
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
				bpSpecificDataSet.addAll((Set<String>) bpDataHash.get(bp));}
			
			Set<String> bpSpecificBLUSet = new HashSet<String>();			
			if (!(bpBLUHash.get(bp) == null)) {
				bpSpecificBLUSet.addAll((Set<String>) bpBLUHash.get(bp));}	
			
			Hashtable<String, Double> dataScoreHash = new Hashtable<String, Double>();
			Hashtable<String, Double> bluScoreHash = new Hashtable<String, Double>();
				
			for (String sys : overallSystemSet) {
				int systemSpecificDataCount = 0, systemSpecificBLUCount = 0;				
			//Figure out what Data Objects a system creates or reads and what BLUs that system provides
				Set<String> systemSpecificDataSet = (Set<String>) systemDataHash.get(sys);
				Set<String> systemSpecificBLUSet = (Set<String>) systemBLUHash.get(sys);
			
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
		logger.info("System: " + sys + ", BP: " + bp + ", Pred: " + pred);
	}

	
	public void setInsertCoreDB(String insertEngine) {
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(insertEngine);
	}
}
