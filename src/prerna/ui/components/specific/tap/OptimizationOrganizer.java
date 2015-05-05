/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

/**
 * This class is used to most efficiently run optimizations for TAP-specific components. 
 * It obtains information about consumer/provider costs and ICD services.
 */
public class OptimizationOrganizer {

	//TODO: standardize the way we define a consumer (should this pull from icds? or system provide data relationship?) in all the queries. Currently discrepancy between the optimization for all systems optimization and system specific optimization.
	
	//Query to determine the LOEs to provide/consume each system for each service. Gets all the central systems that are providing/consuming data objects and the service they can be replaced with. LOEs are broken down for each phase (Design, Develop, Requirements, etc.) of the transition. Does NOT include deployment.
	private String providerCostQuery, consumerCostQuery;
	//Query to determine the generic LOEs  for each service. Gets all the services they can be replaced with. LOEs are broken down for each phase (Design, Develop, Requirements, etc.) of the transition. Includes Deployment.
	private String genericCostQuery;
	
	//Query to determine how many data objects are passed between central systems by looking at the ICDs between central systems. Also calculates the number of services by what what services could expose the data.
	private String icdServiceQuery;
	
	//query to pull all the consumers of a data object. Only pulling from ICDs where type is "TBD". Not sure why this is the case....
	private String consumerSysQuery;
	
	//query to pull the number of ICDs that pass each data object. Pass must occur between central systems through an ICD and must have Type TBD (why?). Also lists the services for that data object.
	private String icdSerCreateQuery;
	private String sysURI = "http://health.mil/ontologies/Concept/System/";

	private static final Logger logger = LogManager.getLogger(OptimizationOrganizer.class.getName());
	private Hashtable<String, Double> serviceHash;
	private Object[][] icdServiceMatrix;
	private ArrayList<String> icdServiceRowNames;
	private ArrayList<String> icdServiceColNames;

	
	public Hashtable<String,Hashtable<String,Double>> detailedServiceCostHash;
	public Hashtable<String, ArrayList<Object[]>> masterHash = new Hashtable<String, ArrayList<Object[]>>();//key is service name; object is system specific information regarding service
	private ArrayList<String> masterServiceList = new ArrayList<String>();

	public ArrayList<String> getICDServiceRowNames() {
		return icdServiceRowNames;
	}
	public ArrayList<String> getICDServiceColNames() {
		return icdServiceColNames;
	}
	public Object[][] getICDServiceMatrix() {
		return icdServiceMatrix;
	}
	public Hashtable<String, Double> getServiceHash() {
		return serviceHash;
	}
	/**
	 * Gets provider, consumer, and generic costs from TAP Core and puts these values into hashtables.
	 * Puts information into the detailed service cost hash with "provider," "consumer," and "generic" as keys for the values.
	 * @param 	Array containing the names of the systems.
	 */
	public void runOrganizer(String system[]){
		setQueries(system);
		serviceHash = new Hashtable<String,Double>();
		Hashtable<String,Double> providerServiceHash = new Hashtable<String,Double>();
		Hashtable<String,Double> consumerServiceHash = new Hashtable<String,Double>();
		Hashtable<String,Double> genericServiceHash = new Hashtable<String,Double>();

		//Get provider costs
		serviceHash = addToHashtable(providerCostQuery, "TAP_Cost_Data", serviceHash, providerServiceHash, true);
		
		//Get consumer costs		
		ArrayList<String> dataSys = getDownStreamConsumers(consumerSysQuery,  "TAP_Core_Data");
		serviceHash = addConsumerToHash(consumerCostQuery, "TAP_Cost_Data", serviceHash,consumerServiceHash, dataSys);
			
		//Get generic costs
		serviceHash = addToHashtable(genericCostQuery, "TAP_Cost_Data", serviceHash, genericServiceHash, false);
		
		//Get icd-ser matrix
		Object[][] icdSerMatrix = createMatrix(icdSerCreateQuery, "TAP_Core_Data");
		fillMatrix(icdServiceQuery, "TAP_Core_Data", icdSerMatrix);

		detailedServiceCostHash = new Hashtable<String,Hashtable<String,Double>>();
		detailedServiceCostHash.put("provider", providerServiceHash);
		detailedServiceCostHash.put("consumer", consumerServiceHash);
		detailedServiceCostHash.put("generic", genericServiceHash);
	}

	/**
	 * Runs a query on the engine to obtain the downstream systems.
	 * Retrieves the system names and associated data and returns this information in an array list.
	 * @param 	Query.
	 * @param 	Name of the engine.
	 * @return 	List containing system names and data. */
	public ArrayList<String> getDownStreamConsumers(String query, String engineName)
	{
		ArrayList<String> retList = new ArrayList<String>();

		ISelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				String dataName = sjss.getVar(names[0])+"";
				String sysName = sjss.getVar(names[1])+"";
				retList.add(dataName+":"+sysName);
			}
		} 
		catch (RuntimeException e) {
			logger.error(e);
		}
		return retList;
	}

	/**
	 * Queries an engine to create a matrix of the appropriate size.
	 * Query should be formatted such that the first variable is the query type, the second is number of rows, and the third is number of columns.
	 * There should be exactly one row returned from the query.
	 * @param 	Query.
	 * @param 	Name of the engine.
	
	 * @return 	Matrix with the appropriate number of rows and columns. */
	private Object[][] createMatrix(String query, String engineName){

		ISelectWrapper wrapper = runQuery(query, engineName);
		String[] names = wrapper.getVariables();
		ISelectStatement sjss = wrapper.next();
		//get the dimensions for the matrix
		int rows = ((Double) sjss.getVar(names[1])).intValue();

		//must override the col number because we can't implement services that do not have consumer costs................
		int cols = masterServiceList.size();

		//make the matrix to return
		Object[][] matrix = new Object[rows][cols];
		return matrix;
	}

	/**
	 * Stores information from the matrix from createMatrix into a hashtable.
	 * @param 	Query to be executed.
	 * @param 	Name of engine that the query is executed upon.
	 * @param 	Matrix containing query outputs.
	
	 * @return 	Hashtable of results originally in the matrix. */
	private void fillMatrix(String query, String engineName, Object[][] matrix){

		icdServiceRowNames = new ArrayList<String>();
		icdServiceColNames = new ArrayList<String>();

		ISelectWrapper wrapper = runQuery(query, engineName);

		//this is going to be processed such that the first col is row name, then column name, then value
		//not going to account for the same cell coming up twice--SPARQL should sum it up

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

				if(sjss.getVar(names[0]) != null)
				{
					//here I need to see if the row name already exists or if the column name already exists
					String rowName = sjss.getVar(names[0])+"";
					String colName = sjss.getVar(names[1])+"";
					Object value = sjss.getVar(names[2]);
					//I need to make sure I am only adding services that have consumer costs... thus must check the masterServiceList
					if(masterServiceList.contains(colName)){
						int rowIdx = icdServiceRowNames.size();
						if(icdServiceRowNames.contains(rowName))
							rowIdx = icdServiceRowNames.indexOf(rowName);
						else icdServiceRowNames.add(rowIdx, rowName);

						int colIdx = icdServiceColNames.size();
						if(icdServiceColNames.contains(colName))
							colIdx = icdServiceColNames.indexOf(colName);
						else icdServiceColNames.add(colIdx, colName);

						//now just put it in the matrix
						matrix[rowIdx][colIdx] = value;
					}
					else {
						logger.warn("Ignoring service because of lack of provider costs: " + colName);
					}

				}
				else {
					break;
				}
			}
		} 
		catch (RuntimeException e) {
			logger.error(e);
		}
		
		logger.info(icdServiceColNames.size()+" out of " +masterServiceList.size());
		//have to put it into a new matrix array because it is possible that not all columns were filled
		icdServiceMatrix = new Object[icdServiceRowNames.size()][icdServiceColNames.size()];
		for(int row = 0; row<icdServiceMatrix.length; row++){
			for (int col = 0; col<icdServiceMatrix[0].length; col++){
				icdServiceMatrix[row][col] = matrix[row][col];
			}
		}
	}

	/**
	 * Adds the provider costs and generic costs to the services hashtable.
	 * @param 	Query.
	 * @param 	Name of engine.
	 * @param 	Service hashtable.
	 * @param 	Provider service hashtable.
	 * @param 	If false, update hashtable with new values.
	 * @return 	Service hashtable with updated values. */
	private Hashtable<String,Double> addToHashtable(String query, String engineName, Hashtable<String,Double> hash, Hashtable<String,Double> hash2, boolean createEntryPrivledges){

		ISelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				if(sjss.getVar(names[0]) != null)
				{
//					String rowName = sjss.getVar(names[0])+"";
					String colName = sjss.getVar(names[1])+"";
					Object value = sjss.getVar(names[2]);
					addToMasterHash(sjss, colName, names);
					//here I need to see if the hashtable already contains the key
					if(!createEntryPrivledges){
						if(hash.containsKey(colName)){
							double prevValue = hash.get(colName);
							double newValue = prevValue + Double.parseDouble(value+"");
							hash.put(colName, newValue);
						}
						else{
							logger.warn("Ignoring service because of lack of provider costs: " + colName);
						}
					}
					else{
						if(hash.containsKey(colName)){
							double prevValue = hash.get(colName);
							double newValue = prevValue + Double.parseDouble(value+"");
							hash.put(colName, newValue);
						}
						else{
							hash.put(colName, Double.parseDouble(value+""));
							masterServiceList.add(colName);
						}
					}

					//add to hash2
					if(hash2!= null&& hash2.containsKey(colName)) {
						double prevValue2 = hash2.get(colName);
						double newValue2 = prevValue2 + Double.parseDouble(value+"");
						hash2.put(colName, newValue2);
					}
					else if(hash2!=null){
						hash2.put(colName, Double.parseDouble(value+""));
					}

				}
				else {
					break;
				}
			}
		} 
		catch (RuntimeException e) {
			logger.error(e);
		}

		return hash;
	}
	/**
	 * Adds consumer services to a hashtable.
	 * Runs a query on a specific engine and goes through the wrapper to extract system/data names.
	 * Checks to see whether column names exist as keys in either hashtable and updates keys with new values if applicable.
	 * @param 	Query.
	 * @param 	Name of engine.
	 * @param 	Hashtable of services.
	 * @param 	Hashtable of consumer services.
	 * @param 	List of systems.
	
	 * @return 	Hashtable with updated values. */
	private Hashtable<String,Double> addConsumerToHash(String query, String engineName, Hashtable<String,Double> hash, Hashtable<String,Double> hash2, ArrayList<String> sysList){

		ISelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				if(sjss.getVar(names[0]) != null)
				{
					String sysName = sjss.getVar(names[0])+"";
					String dataName = sjss.getVar(names[6])+"";
					String conc =dataName+":"+sysName ;
					if (sysList.contains(conc))
					{
//						String rowName = sjss.getVar(names[0])+"";
						String colName = sjss.getVar(names[1])+"";
						Object value = sjss.getVar(names[2]);
						addToMasterHash(sjss, colName, names);
						//here I need to see if the hashtable already contains the key

						if(hash.containsKey(colName)){
							double prevValue = (Double) hash.get(colName);
							double newValue = prevValue + Double.parseDouble(value+"");
							hash.put(colName, newValue);
						}
						else{
							logger.warn("Ignoring service because of lack of provider costs: " + colName);
						}


						//add to hash2

						if(hash2!= null&& hash2.containsKey(colName)) {
							double prevValue2 = (Double) hash2.get(colName);
							double newValue2 = prevValue2 + Double.parseDouble(value+"");
							hash2.put(colName, newValue2);
						}
						else if(hash2!=null){
							hash2.put(colName, Double.parseDouble(value+""));
						}
					}
				}
				else {
					break;
				}
			}
		} 
		catch (RuntimeException e) {
			logger.error(e);
		}
		
		return hash;
	}
	/**
	 * Stores the RDF triples from the Select Statement into a new row.
	 * Checks if a service name is already in the master hash and adds the new row to the hash accordingly.
	 * @param 	SesameJenaSelectStatement containing the triples.
	 * @param 	Name of the service.
	 * @param 	List of existing names of services.
	 */
	private void addToMasterHash(ISelectStatement sjss, String serName, String[] names){
		//first create the new row of the table
		Object[] newRow = new Object[names.length];
		for(int nameIdx = 0; nameIdx<names.length; nameIdx++){
			Object obj = sjss.getVar(names[nameIdx]);
			newRow[nameIdx] = obj;
		}
		
		if(masterHash.containsKey(serName)){
			ArrayList<Object[]> table = masterHash.get(serName);
			table.add(newRow);
			masterHash.put(serName, table);
		}
		else{
			ArrayList<Object[]> table = new ArrayList<Object[]>();
			table.add(newRow);
			masterHash.put(serName, table);	
		}
	}
	
	/**
	 * Runs a query on a specified engine.
	 * @param 	Query.
	 * @param 	Engine name.
	
	 * @return 	Wrapper that processes SELECT statements. */
	private ISelectWrapper runQuery(String query, String engineName){
		
		Object[] repo = new Object[]{engineName};
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		*/
		
		return wrapper;
	}
	
	/**
	 * Given a list of systems, set the queries for provider/consumer costs, ICD services, and consumer systems.
	 * @param 	List of systems.
	 */
	public void setQueries(String[] system)
	{
		providerCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?element WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem ;}{?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase ;} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?element <http://semoss.org/ontologies/Relation/Input> ?GLitem} }";
		consumerCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?inputElement WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Consumer> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase ;} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?inputElement <http://semoss.org/ontologies/Relation/Input> ?GLitem}}";
		genericCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?inputElement WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) BIND(\"Generic\" as ?sys) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase ;} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;}  {?inputElement <http://semoss.org/ontologies/Relation/Input> ?GLitem} }";
		icdSerCreateQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT(?data)) AS ?dataCount) (COUNT(DISTINCT(?ser)) AS ?serCount) WHERE { BIND(\"Provider Count\" AS ?type) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}  {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?sys2 ?OwnedBy2 <http://health.mil/ontologies/Concept/SystemOwner/Central> }  {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data;} {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?sys <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?sys2} } GROUP BY ?type";
		if (system==null)
		{
			consumerSysQuery="SELECT DISTINCT ?Data1 ?System3 WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}{?System1 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }{?System3 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }{?System1 <http://semoss.org/ontologies/Relation/Provide> ?icd1 ;}{?icd1 <http://semoss.org/ontologies/Relation/Consume> ?System3;}{?icd1 ?carries ?Data1;}{?carries <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"}}";
			icdServiceQuery = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data;} {?icd ?pay ?data}  {?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?sys2 ?OwnedBy2 <http://health.mil/ontologies/Concept/SystemOwner/Central>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?sys2}  BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer";
		}
		else
		{
			String bindSysStr = getBindingString(system);
			providerCostQuery = providerCostQuery + "BINDINGS ?sys {"+bindSysStr+"}";	
			consumerSysQuery="SELECT DISTINCT ?Data1 ?System3 WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;}   {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm ;} {?System3 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?System1 ?provide ?Data1 ;} {?System1 <http://semoss.org/ontologies/Relation/Provide> ?icd1 ;}{?icd1 <http://semoss.org/ontologies/Relation/Consume> ?System3;} {?icd1 ?carries ?Data1;}{?carries <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} FILTER (?crm in(\"C\",\"M\"))} BINDINGS ?System1 {"+bindSysStr+"}";
			icdServiceQuery = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data;} {?icd ?pay ?data} {?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}  {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?sys2 ?OwnedBy2 <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?sys <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?sys2} {?sys ?provide2 ?data} {?provide2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?provide2 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}FILTER (?CRM in(\"C\",\"M\"))BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer BINDINGS ?sys {"+bindSysStr+"} ";
		}
	}
	
	/**
	 * Iterate through a list of systems and use processing to retrieve the binding string for each.
	 * @param 	List of systems.
	 * @return 	Binding string. */
	public String getBindingString(String[] sysArray)
	{
		String retString = "";
		for(int i = 0;i < sysArray.length;i++) {
			retString = retString + "(<" + sysURI + sysArray[i] + ">)";
		}
		return retString;
	}
	
}
