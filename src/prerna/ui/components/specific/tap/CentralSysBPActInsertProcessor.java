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

import javax.swing.JList;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.rdbms.schema.HashTable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CentralSysBPActInsertProcessor {
	
	Logger logger = Logger.getLogger(getClass());
	private IEngine coreDB;
	private String tapCoreEngine = "TAP_Core_Data";
	private double dataObjectThresholdValue = 0.0;
	private double bluThresholdValue = 0.0;
	private String semossBaseURI = "http://semoss.org/ontologies/Concept/";
	private String semossRelBaseURI = "http://semoss.org/ontologies/Relation/";
	private String propURI = "http://semoss.org/ontologies/Relation/Contains/";

	private Hashtable<String, Hashtable<String, Object>> dataHash = new Hashtable<String, Hashtable<String, Object>>();
	private Hashtable<String, Hashtable<String, Object>> removeDataHash = new Hashtable<String, Hashtable<String, Object>>();

	private Hashtable<String, Set<String>> newRelations = new Hashtable<String, Set<String>>();
	private Hashtable<String, Set<String>> allConcepts = new Hashtable<String, Set<String>>();

	public String errorMessage = "";
	
	private String TAP_CORE_CENTRAL_SYSTEM_LIST_QUERY = "SELECT DISTINCT ?System WHERE { BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System} }";
	//TODO Ask Bill or George
	//private String TAP_CORE_CENTRAL_SYSTEM_LIST_QUERY = "SELECT DISTINCT ?System WHERE { BIND(<http://health.mil/ontologies/Concept/SystemOwner/Central> AS ?SystemOwner) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  {?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner} }";
	
	private String TAP_CORE_BUSINESS_PROCESSES_QUERY = "SELECT DISTINCT ?BusinessProcess WHERE { {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} }";
	private String TAP_CORE_BUSINESS_PROCESSES_ACTIVITY_QUERY = "SELECT DISTINCT ?BusinessProcess ?Activity WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?ConsistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?BusinessProcess ?ConsistsOf ?Activity} }";
	private String TAP_CORE_ACTIVITY_DATA_QUERY = "SELECT DISTINCT ?Activity ?Data WHERE {{?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Activity ?Needs ?Data.} }";
	private String TAP_CORE_ACTIVITY_BLU_QUERY = "SELECT DISTINCT ?Activity ?BLU WHERE { {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Activity ?Needs ?BLU.} }";	
	private String TAP_CORE_SYSTEM_DATA_QUERY = "SELECT DISTINCT ?System ?Data ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System}}";
	private String TAP_CORE_SYSTEM_BLU_QUERY = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?provide ?BLU.} BIND(<http://health.mil/ontologies/Concept/Tasker/TAP_Central_Systems_Tasker> AS ?Tasker) {?DistributedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DistributedTo> ;}{?Tasker ?DistributedTo ?System} }";
	
	public String getErrorMessage() {
		return this.errorMessage;
	}

	public CentralSysBPActInsertProcessor(Double dataObjectThresholdValue, Double bluThresholdValue) {
		this.dataObjectThresholdValue = dataObjectThresholdValue;
		this.bluThresholdValue = bluThresholdValue;
		this.coreDB = (IEngine) DIHelper.getInstance().getLocalProp(tapCoreEngine);
	}
	
	public boolean runAggregation()	{
		boolean success = true;
		logger.info("Data Object Threshold Value = " + dataObjectThresholdValue*100 + "%");
		logger.info("Business Logic Unit Threshold Value = " + bluThresholdValue*100 + "%");
		logger.info("Core DB is: " + coreDB);
		
		//2.  Collect Sys==>Data (CRM) and Sys==>BLU 
		//3.  Collect Act==>Data (CRM) and Act==> BLU     Collect Act==>BP
		//ArrayList<ArrayList<Object>> centralSystemList = runQuery(tapCoreEngine ,TAP_CORE_CENTRAL_SYSTEM_LIST_QUERY);
		ArrayList<ArrayList<Object>> systemData = runQuery(tapCoreEngine, TAP_CORE_SYSTEM_DATA_QUERY);
		ArrayList<ArrayList<Object>> systemBLU = runQuery(tapCoreEngine, TAP_CORE_SYSTEM_BLU_QUERY);
		ArrayList<ArrayList<Object>> activityData = runQuery(tapCoreEngine, TAP_CORE_ACTIVITY_DATA_QUERY);
		ArrayList<ArrayList<Object>> activityBLU = runQuery(tapCoreEngine, TAP_CORE_ACTIVITY_BLU_QUERY);
		ArrayList<ArrayList<Object>> bpActivity = runQuery(tapCoreEngine, TAP_CORE_BUSINESS_PROCESSES_ACTIVITY_QUERY);
		ArrayList<ArrayList<Object>> bpList = runQuery(tapCoreEngine, TAP_CORE_BUSINESS_PROCESSES_QUERY);
		
		//Key = System, Value = Count of Data Objects Created
		Hashtable systemDataHash = new Hashtable();
		//Key = System, Value = Count of BLUs Provided
		Hashtable systemBLUHash = new Hashtable();
		
		
		HashSet<String> overallActivitySet = new HashSet<String>();
		HashSet<String> overallSystemSet = new HashSet<String>();
		//HashSet<String> dataSet = new HashSet<String>();
		//HashSet<String> bluSet = new HashSet<String>();
		//populate the System, Activity-Data and Activity-BLU sets with the data from the Array Lists		
		for (int i = 0; i < activityBLU.size(); i++) {
			ArrayList tempList = (ArrayList) activityBLU.get(i);
			overallActivitySet.add((String) tempList.get(0));		
		}
		for (int i = 0; i < activityData.size(); i++) {
			ArrayList tempList = (ArrayList) activityData.get(i);
			overallActivitySet.add((String) tempList.get(0));		
		}
		for (int i = 0; i < systemBLU.size(); i++) {
			ArrayList tempList = (ArrayList) systemBLU.get(i);
			overallSystemSet.add((String) tempList.get(0));		
		}
		for (int i = 0; i < systemData.size(); i++) {
			ArrayList tempList = (ArrayList) systemData.get(i);
			overallSystemSet.add((String) tempList.get(0));		
		}
			
		System.out.println(overallActivitySet.size());
		
		for (String a : overallActivitySet) {
			//Key = Activity, Value = Count of Data/BLU
			ArrayList<String> activitySpecificDataList = new ArrayList<String>();
			ArrayList<String> activitySpecificBLUList = new ArrayList<String>();	
			
			//Process Query Results (Activity Specific)
			for (int i = 0; i < activityData.size(); i++) {
				ArrayList tempList = (ArrayList) activityData.get(i);
				//System.out.println((tempList.get(0)).equals(a));
				if ((tempList.get(0)).equals(a)) { 
					activitySpecificDataList.add((String) tempList.get(1));
				}
			}	
			for (int i = 0; i < activityBLU.size(); i++) {
				ArrayList tempList = (ArrayList) activityBLU.get(i);
				//System.out.println((tempList.get(0)).equals(a));
				if ((tempList.get(0)).equals(a)) { 
					activitySpecificBLUList.add((String) tempList.get(1));
				}
			}		
		//}test
			for (String s : overallSystemSet) {
				int systemSpecificDataCount = 0, systemSpecificBLUCount = 0;
			//Figure out what Data Objects a system creates or reads
				ArrayList<String> systemSpecificCreatedDataList = new ArrayList<String>();
				for (int i = 0; i < systemData.size(); i++) {
					ArrayList tempList = (ArrayList) systemData.get(i);
					if (tempList.get(0).equals(s) & ((String) tempList.get(2)).equals("C")) { 	
						systemSpecificCreatedDataList.add((String) tempList.get(1));
					}
				}	
			//Figure out what BLUs a system provides
				ArrayList<String> systemSpecificBLUList = new ArrayList<String>();
				for (int i = 0; i < systemBLU.size(); i++) {
					ArrayList tempList = (ArrayList) systemBLU.get(i);
					if (tempList.get(0).equals(s)) {
						systemSpecificBLUList.add((String) tempList.get(1));
					}
				}
				
						
				for (String d : systemSpecificCreatedDataList) {
					//check to see if 'd' is needed by the activity 'a' 
					if (activitySpecificBLUList.contains(d)) {
						systemSpecificBLUCount++;
					}
				}
				for (String b : systemSpecificBLUList) {
					//check to see if 'b' is needed by the activity 'a'
					if (activitySpecificBLUList.contains(b)) {
						systemSpecificBLUCount++;
					}
				} 
			//Decide if System 's' Supports Activity	
				if ( (systemSpecificDataCount > ( activitySpecificDataList.size()*dataObjectThresholdValue ))
						& (systemSpecificBLUCount > ( activitySpecificBLUList.size()*bluThresholdValue )) ) {
					//Add the activity-System relation
					System.out.println(s);
				}
			}
		}
		
		//For Each Activity {
			//For each Data Object {
				//Increase Activity-Data count +1
				//Get Systems that C/R and increase data count +1
			//}
			//For each BLU {
				//Increase Activity-BLU count +1
				//Get Systems that provide and increase BLU count +1
			//}
			//For each stored system {
				//Does BLU and data count pass the threshold?
					//Yes==> Store System and activity in new relationship Hashtable
			//}
		//}
		
		//Insert new relationships (Full URIs)
		
		
		return success;		
	}
	
	private Object[] processConcatString(String sub, String prop, Object value, String user) {
		// replace any tags for properties that are loaded as other data types but should be strings
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#double","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#decimal","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#integer","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#float","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#boolean","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#dateTime","");

		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))	{
			if(!user.equals(""))	{
				value = "\"" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString().substring(1);
			}
			logger.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else {
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			if(!user.equals("")) {
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString().substring(1);
			}
			else {
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + value.toString().substring(1);
			}
			logger.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}
	
	//Utility Methods
	private String getTextAfterFinalDelimeter(String uri, String delimeter)	{
		if(!uri.equals(""))
		{
			uri = uri.substring(uri.lastIndexOf(delimeter)+1);
		}
		return uri;
	}
	
	//process the query
	private SesameJenaSelectWrapper processQuery(String query, IEngine engine){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}
	
	/**
	 * Runs a query on a specific database and returns the result as an ArrayList
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list 		ArrayList<ArrayList<Object>> containing the results of the query 
	 */
	public ArrayList runQuery(String engineName, String query) {
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		
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
				ArrayList<Object> values = new ArrayList<Object>();
				for (int colIndex = 0; colIndex < names.length; colIndex++) {
					if (sjss.getVar(names[colIndex]) != null) {
						if (sjss.getVar(names[colIndex]) instanceof Double) {
							values.add(colIndex, (Double) sjss.getVar(names[colIndex]));
						}
						else values.add(colIndex, (String) sjss.getVar(names[colIndex]));						
					}
				}
				list.add(values);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	private Hashtable runQueryTest(String query, String engineName) {
		Hashtable queryDataHash = new Hashtable ();
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		SesameJenaSelectWrapper sjsw = processQuery(query, engine);
		String[] variables = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			
			// get the next row and see how it must be added to the insert query
//			String subject = sjss.getRawVar(vars[0]).toString();
//			String pred = sjss.getRawVar(vars[1]).toString();
//			String object = sjss.getRawVar(vars[2]).toString();
			
			//addToHash(new String[]{subject, pred, object});		
			
		}
		return queryDataHash;
		
	}
	
	//TODO: Need something similar to this method to insert the new relationships
	/*private void runRelationshipAggregation(String query) {
		dataHash.clear();
		SesameJenaSelectWrapper sjsw = processQuery(query, servicesDB);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String subject = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String object = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(subject, "/") +":" + getTextAfterFinalDelimeter(object, "/");
			logger.debug("ADDING RELATIONSHIP:     " + subject + " -----> {" + pred + " --- " + object + "}");
			addToHash(new String[]{subject, pred, object});
			// add instances to master list
			addToAllConcepts(subject);
			addToAllConcepts(object);
			addTonewRelationships(pred);
		}
		processData(dataHash);
	}*/
	
	
	//Methods to Insert New Relationships
	private void addToNewRelationships(String uri)	{
		String relationBaseURI = semossRelBaseURI + Utility.getClassName(uri);
		if(newRelations.containsKey(relationBaseURI))	{
			newRelations.get(relationBaseURI).add(uri);
		}
		else {
			newRelations.put(relationBaseURI, new HashSet<String>());
			newRelations.get(relationBaseURI).add(uri);
		}
	}
	private void processNewRelationships() {
		String relation = "http://semoss.org/ontologies/Relation";
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
		for ( String obj : newRelations.keySet()) {
			for (String sub : newRelations.get(obj) ) {
				( (BigDataEngine) coreDB).addStatement(sub, subpropertyOf, obj, true);
				logger.info("ADDING RELATIONSHIP INSTANCE SUBPROPERTY TRIPLE: " + sub + ">>>>>" + subpropertyOf + ">>>>>" + obj + ">>>>>");
			}
		}	
	}
	
	/**
	 * Set the engine for the entire class
	 * @param engineName	String containing the name of the engine
	 */
	public void setEngine(String engineName){
		this.coreDB = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
	}

}
