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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SORpropInsertProcessor extends AggregationHelper {
	
	private static final Logger LOGGER = LogManager.getLogger(SORpropInsertProcessor.class.getName());
	private IEngine coreDB;
	
	private String tapCoreBaseURI = "http://health.mil/ontologies/Relation/";
	
	public String errorMessage = "";
		
	public String SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?provide ?Data .} }";
	public String SYSTEM_DATA_SOR_QUERY = "SELECT DISTINCT ?system ?data WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?data ?system";
	//private String DELETE_NEW_RELATIONS_QUERY = "SELECT ?System ?relation ?o ?allInferredRelationships WHERE { {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> 'yes'} MINUS {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> 'yes' } {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?DataObject} {?relation ?allInferredRelationships ?o}}";
	// String DELETE_NEW_PROPERTIES_QUERY = "SELECT ?relation ?pred ?Calculated WHERE {BIND(<http://semoss.org/ontologies/Relation/Contains/Calculated> AS ?pred) {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?relation <http://semoss.org/ontologies/Relation/Contains/Reported> ?Reported}  {?relation <http://semoss.org/ontologies/Relation/Contains/Calculated> ?Calculated} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System ?relation ?o}} ";
	
	public String getErrorMessage() {
		return this.errorMessage;
	}

	public SORpropInsertProcessor() {
		// TODO Auto-generated constructor stub
	}
	
	/*public void runDeleteQueries() {
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
	}*/
	
	public boolean runCoreInsert() {
		boolean methodSuccess = true;
		LOGGER.info("Core DB is: " + coreDB.getEngineName());
		
		//1. Query and store the Data
		Hashtable<String, Set<String>> systemDataHash = getQueryResultHash(coreDB, SYSTEM_DATA_QUERY);
		Hashtable<String, Set<String>> systemDataSORHash = getQueryResultHash(coreDB, SYSTEM_DATA_SOR_QUERY);
		
		if (!(coreDB.getEngineName().equals("TAP_Core_Data"))) {
			this.errorMessage = "Select TAP_Core_Data from the database list.";
			return false;
		}				
		if (systemDataHash.isEmpty()) {
			this.errorMessage = "One or more of the queries returned no results.";
			return false;
		}
		//2. Processing and Analysis
		allRelations.clear();
		dataHash.clear();
		
		processRelations(systemDataHash, systemDataSORHash);
		
		//3.  Insert new relationships (Full URIs)	
	    processData(coreDB, dataHash);
	    for (String obj : allRelations.keySet()) {
	    	for (String sub : allRelations.get(obj)) {
	    	   processNewRelationshipsAtInstanceLevel(coreDB, sub, obj);
	    	}
	    }
	 	((BigDataEngine) coreDB).infer();
	 	
		return methodSuccess;		
	}
	
	private void processRelations(Hashtable<String, Set<String>> systemDataHash, Hashtable<String, Set<String>> systemDataSORHash) {		
				
		Vector<String> sorSysDataV = concatHashToVector(systemDataSORHash);
		Vector<String> sysDataV = concatHashToVector(systemDataHash);
		String system = "";
		String data = "";
		
		if (system.contains("NSIPS")) {
			System.out.println("HERE");
		}
		
		//Add SOR "Yes"
		for (String sorCombo : sorSysDataV) {
			StringTokenizer concatResult = new StringTokenizer(sorCombo, "@");			
			for (int queryIdx = 0; concatResult.hasMoreTokens(); queryIdx++){
				String token = concatResult.nextToken();
				if (queryIdx == 0){
					system = token;
				}
				else if (queryIdx == 1){
					data = token;
				}
			}
			
			if (sysDataV.contains(sorCombo)) {
				//add property SOR to relationship				
				sorRelationProcessing(system, data, false, false);
			}
			else {
				//Add new relationship System->Provides->Data with SOR and Calculated Properties = "Yes"
				sorRelationProcessing(system, data, true, false);
			}
		}
		
		//Add SOR "no
		for (String sysData : sysDataV) {
			StringTokenizer concatResult = new StringTokenizer(sysData, "@");			
			for (int queryIdx = 0; concatResult.hasMoreTokens(); queryIdx++){
				String token = concatResult.nextToken();
				if (queryIdx == 0){
					system = token;
				}
				else if (queryIdx == 1){
					data = token;
				}
			}
			if (!(sorSysDataV.contains(sysData))) {
				sorRelationProcessing(system, data, false, true);
			}
		}
		
		
		addToDataHash(new Object[]{semossPropertyBaseURI + "SOR", RDF.TYPE.toString(), semossRelationBaseURI + "Contains"});
		//addToDataHash(new Object[]{semossPropertyBaseURI + "Reported", RDF.TYPE.toString(), semossRelationBaseURI + "Contains"});
		//logger.info("*****SubProp URI: "+ semossPropertyBaseURI + "Calculated" + " typeURI : " + RDF.TYPE.toString() + " propbase: " + semossRelationBaseURI + "Contains");				
	}
	
	private Vector<String> concatHashToVector(Hashtable<String, Set<String>> concatHashTable) {
		Vector<String> concatV = new Vector<String>();
		String concatValue = "";
		
		Set<String> concatHashKeySet = new HashSet<String>();
		concatHashKeySet.addAll(concatHashTable.keySet());
				
		for (String key : concatHashKeySet) {
			Set<String> values = concatHashTable.get(key);
			for (String value : values) {
				concatValue = key + "@" + value;
				concatV.add(concatValue);
			}
		}
		
		return concatV;
	}
	
	public void sorRelationProcessing(String sys, String dataObject, boolean notfound, boolean notSOR) {					
		String pred = tapCoreBaseURI + "Provide";
		pred = pred + "/" + getTextAfterFinalDelimeter(sys, "/") +":" + getTextAfterFinalDelimeter(dataObject, "/");
		if (notfound) {
			//add the new relationship
			addToDataHash(new Object[]{sys, pred, dataObject});
			addToDataHash(new Object[]{pred, semossPropertyBaseURI + "Reported", "No"});
		}
		//add the new properties		
		if (notSOR) {
			addToDataHash(new Object[]{pred, semossPropertyBaseURI + "SOR", "No"});
		}
		else {
			addToDataHash(new Object[]{pred, semossPropertyBaseURI + "SOR", "Yes"});
		}
		//logger.info("*****Prop URI: " + pred + ", predURI: " + semossPropertyBaseURI + "Calculated" + ", value: " + "yes");
		addToAllRelationships(pred);					
		LOGGER.info("System: " + sys + ", Data: " + dataObject + ", Pred: " + pred);
	}
	
	public void setInsertCoreDB(String insertEngine) {
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(insertEngine);
	}
	
	public Hashtable<String, Set<String>> getQueryResultHash(IEngine db, String query) {
		ISelectWrapper queryDataWrapper = Utility.processQuery(db, query);
		Hashtable<String, Set<String>> queryDataHash = hashTableResultProcessor(queryDataWrapper);
		return queryDataHash;
	}
	
	public Hashtable<String, Set<String>> hashTableResultProcessor(ISelectWrapper sjsw) {
		Hashtable<String, Set<String>> aggregatedData = new Hashtable<String, Set<String>>();
		String[] vars = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();			
			String sub = sjss.getRawVar(vars[0]).toString();
			Set<String> pred = new HashSet<String>();
			pred.add(sjss.getRawVar(vars[1]).toString());
			if (!aggregatedData.containsKey(sub))
				{aggregatedData.put(sub, pred);}
			else {aggregatedData.get(sub).add(sjss.getRawVar(vars[1]).toString());}				
		}						
		return aggregatedData;
	}

}
