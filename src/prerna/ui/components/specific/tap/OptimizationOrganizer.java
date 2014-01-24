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
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

/**
 * This class is used to most efficiently run optimizations for TAP-specific components. 
 * It obtains information about consumer/provider costs and ICD services.
 */
public class OptimizationOrganizer {

	String providerCostQuery;
	String consumerCostQuery;
	String genericCostQuery;
	String icdServiceQuery;
	String consumerSysQuery;
	String providerCostCreateQuery;
	String consumerCostCreateQuery;
	String genericCostCreateQuery;
	String icdSerCreateQuery;

	public String colLabel = "COLUMN_LABEL";
	public String rowLabel = "ROW_LABEL";
	public String matrixLabel = "MATRIX_LABEL";
	Logger logger = Logger.getLogger(getClass());
	public Hashtable<String, Double> serviceHash;
	public Hashtable icdService;
	public Hashtable detailedServiceCostHash;
	public Hashtable<String, ArrayList<Object[]>> masterHash = new Hashtable();//key is service name; object is system specific information regarding service
	ArrayList<String> masterServiceList = new ArrayList<String>();

	//CURRENTLY INGORING DEPLOYMENT COSTS on consumer
	/**
	 * Gets provider, consumer, and generic costs from TAP Core and puts these values into hashtables.
	 * Puts information into the detailed service cost hash with "provider," "consumer," and "generic" as keys for the values.
	 * @param 	Array containing the names of the systems.
	 */
	public void runOrganizer(String system[]){
		setQueries(system);
		serviceHash = new Hashtable();
		Hashtable providerServiceHash = new Hashtable();
		Hashtable consumerServiceHash = new Hashtable();
		Hashtable genericServiceHash = new Hashtable();

		//Get provider costs
		//Object[][] providerMatrix = createMatrix(providerCostCreateQuery, "TAP_Cost_Data");
		serviceHash = addToHashtable(providerCostQuery, "TAP_Cost_Data", serviceHash, providerServiceHash, true);
		//Get consumer costs
		//Object[][] consumerMatrix = createMatrix(consumerCostCreateQuery, "TAP_Cost_Data");
		
		ArrayList dataSys = getDownStreamConsumers(consumerSysQuery,  "TAP_Core_Data");
		serviceHash = addConsumerToHash(consumerCostQuery, "TAP_Cost_Data", serviceHash,consumerServiceHash, dataSys);
			
		//Get generic costs
		//Object[][] genericMatrix = createMatrix(genericCostCreateQuery, "TAP_Cost_Data");
		serviceHash = addToHashtable(genericCostQuery, "TAP_Cost_Data", serviceHash, genericServiceHash, false);
		//Get icd-ser matrix
		Object[][] icdSerMatrix = createMatrix(icdSerCreateQuery, "TAP_Core_Data");
		icdService = fillMatrix(icdServiceQuery, "TAP_Core_Data", icdSerMatrix);

		detailedServiceCostHash = new Hashtable();
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
	public ArrayList getDownStreamConsumers(String query, String engineName)
	{
		ArrayList retList = new ArrayList();


		SesameJenaSelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				String sysName = sjss.getVar(names[1])+"";
				String dataName = sjss.getVar(names[2])+"";
				retList.add(sysName+":"+dataName);
			}
		} 
		catch (Exception e) {
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

		SesameJenaSelectWrapper wrapper = runQuery(query, engineName);
		String[] names = wrapper.getVariables();
		SesameJenaSelectStatement sjss = wrapper.next();
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
	private Hashtable fillMatrix(String query, String engineName, Object[][] matrix){
		Hashtable hash = new Hashtable();
		ArrayList<String> rowNames = new ArrayList<String>();
		ArrayList<String> colNames = new ArrayList<String>();

		SesameJenaSelectWrapper wrapper = runQuery(query, engineName);

		//this is going to be processed such that the first col is row name, then column name, then value
		//not going to account for the same cell coming up twice--SPARQL should sum it up

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				if(sjss.getVar(names[0]) != null)
				{
					//here I need to see if the row name already exists or if the column name already exists
					String rowName = sjss.getVar(names[0])+"";
					String colName = sjss.getVar(names[1])+"";
					Object value = sjss.getVar(names[2]);
					//I need to make sure I am only adding services that have consumer costs... thus must check the masterServiceList
					if(masterServiceList.contains(colName)){
						int rowIdx = rowNames.size();
						if(rowNames.contains(rowName))
							rowIdx = rowNames.indexOf(rowName);
						else rowNames.add(rowIdx, rowName);

						int colIdx = colNames.size();
						if(colNames.contains(colName))
							colIdx = colNames.indexOf(colName);
						else colNames.add(colIdx, colName);

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
		catch (Exception e) {
			logger.error(e);
		}
		
		logger.info(colNames.size()+" out of " +masterServiceList.size());
		//have to put it into a new matrix array because it is possible that not all columns were filled
		Object[][] finalMatrix = new Object[rowNames.size()][colNames.size()];
		for(int row = 0; row<finalMatrix.length; row++){
			for (int col = 0; col<finalMatrix[0].length; col++){
				finalMatrix[row][col] = matrix[row][col];
			}
		}

		//now store in the hash and pass it back
		hash.put(rowLabel, rowNames);
		hash.put(colLabel, colNames);
		hash.put(matrixLabel, finalMatrix);
		return hash;
	}

	/**
	 * Adds the provider costs and generic costs to the services hashtable.
	 * @param 	Query.
	 * @param 	Name of engine.
	 * @param 	Service hashtable.
	 * @param 	Provider service hashtable.
	 * @param 	If false, update hashtable with new values.
	
	 * @return 	Service hashtable with updated values. */
	private Hashtable addToHashtable(String query, String engineName, Hashtable hash, Hashtable hash2, boolean createEntryPrivledges){

		SesameJenaSelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();


				if(sjss.getVar(names[0]) != null)
				{
					String rowName = sjss.getVar(names[0])+"";
					String colName = sjss.getVar(names[1])+"";
					Object value = sjss.getVar(names[2]);
					addToMasterHash(sjss, colName, names);
					//here I need to see if the hashtable already contains the key
					if(!createEntryPrivledges){
						if(hash.containsKey(colName)){
							double prevValue = (Double) hash.get(colName);
							double newValue = prevValue + Double.parseDouble(value+"");
							hash.put(colName, newValue);
						}
						else{
							logger.warn("Ignoring service because of lack of provider costs: " + colName);
						}
					}
					else{
						if(hash.containsKey(colName)){
							double prevValue = (Double) hash.get(colName);
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
						double prevValue2 = (Double) hash2.get(colName);
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
		catch (Exception e) {
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
	private Hashtable addConsumerToHash(String query, String engineName, Hashtable hash, Hashtable hash2, ArrayList sysList){

		SesameJenaSelectWrapper wrapper = runQuery(query, engineName);

		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();


				if(sjss.getVar(names[0]) != null)
				{
					String sysName = sjss.getVar(names[0])+"";
					String dataName = sjss.getVar(names[6])+"";
					String conc =dataName+":"+sysName ;
					if (sysList.contains(conc))
					{
						String rowName = sjss.getVar(names[0])+"";
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
		catch (Exception e) {
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
	private void addToMasterHash(SesameJenaSelectStatement sjss, String serName, String[] names){
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
			ArrayList<Object[]> table = new ArrayList();
			table.add(newRow);
			masterHash.put(serName, table);	
		}
	}
	
	/**
	 * Runs a query on a specified engine.
	 * @param 	Query.
	 * @param 	Engine name.
	
	 * @return 	Wrapper that processes SELECT statements. */
	private SesameJenaSelectWrapper runQuery(String query, String engineName){
		
		Object[] repo = new Object[]{engineName};
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		
		return wrapper;
	}
	
	/**
	 * Given a list of systems, set the queries for provider/consumer costs, ICD services, and consumer systems.
	 * @param 	List of systems.
	 */
	public void setQueries(String[] system)
	{
		providerCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?element WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?sys <http://www.w3.org/2000/01/rdf-schema#label> ?SysName}{?sys ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs) {?GLitem ?belongs ?phase ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Output> AS ?output) {?GLitem ?output ?ser ;}{?ser <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?element ?input ?GLitem} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }}";	
		consumerCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?inputElement WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Consumer> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences){?sys ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs)  {?GLitem ?belongs ?phase ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Output> AS ?output) {?GLitem ?output ?ser ;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?inputElement ?input ?GLitem} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }}";
		genericCostQuery = "SELECT DISTINCT ?sys ?ser ?loe ?GLitem ?phase ?gltag ?inputElement WHERE {  {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs) {?GLitem ?belongs ?phase ;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?inputElement ?input ?GLitem} BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag). BIND( <http://semoss.org/ontologies/Relation/Output> AS ?output) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?GLitem ?output ?ser ;}  {?ser <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BIND(\"Generic\" as ?sys)}";
		icdSerCreateQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT(?data)) AS ?dataCount) (COUNT(DISTINCT(?ser)) AS ?serCount) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND( <http://semoss.org/ontologies/Relation/Exposes> AS ?exp) {?ser ?exp ?data;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} BIND( <http://semoss.org/ontologies/Relation/Payload> AS ?pay) {?icd ?pay ?data} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?sys ?provide ?icd} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?consume) {?sys2 ?OwnedBy2 <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?icd ?consume ?sys2} BIND(\"Provider Count\" AS ?type) } GROUP BY ?type";
		if (system==null)
		{
			consumerSysQuery="SELECT DISTINCT (\"y\" AS ?y) ?Data1 ?System3 WHERE { {?System1 ?upstream1 ?icd1 ;}{?System1 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?upstream1) BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream1) {?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;}{?carries <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"}{?System3 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> }}";
			icdServiceQuery = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND( <http://semoss.org/ontologies/Relation/Exposes> AS ?exp) {?ser ?exp ?data;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}  {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?pay ?data}  {?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"}{?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?sys ?provide ?icd} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?consume) {?sys2 ?has2 <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?icd ?consume ?sys2} BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer ";
		}
		else
		{
			String bindSysStr = getBindingString(system);
			providerCostQuery = providerCostQuery + "BINDINGS ?SysName {"+bindSysStr+"}";	
			consumerSysQuery="SELECT DISTINCT ?System1 ?Data1 ?System3 WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?System1 <http://www.w3.org/2000/01/rdf-schema#label> ?SysName} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System1 ?provide ?Data1 ;} BIND(<http://semoss.org/ontologies/Relation/Contains/CRM> AS ?contains2). {?provide ?contains2 ?crm ;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System1 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;}{?carries <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"}{?System3 ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } FILTER (?crm in(\"C\",\"M\"))}BINDINGS ?SysName {"+bindSysStr+"}";
			icdServiceQuery = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND( <http://semoss.org/ontologies/Relation/Exposes> AS ?exp) {?ser ?exp ?data;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}  {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?sys <http://www.w3.org/2000/01/rdf-schema#label> ?SysName} {?icd ?pay ?data}{?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?sys ?provide ?icd} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?consume) {?sys2 ?OwnedBy2 <http://health.mil/ontologies/Concept/SystemOwner/Central> } {?icd ?consume ?sys2} {?sys ?provide2 ?data} {?provide2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?provide2 <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}FILTER (?CRM in(\"C\",\"M\"))BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer BINDINGS ?SysName {"+bindSysStr+"} ";
		}
		
		/*providerCostCreateQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT(?sys)) AS ?sysCount) (COUNT(DISTINCT(?ser)) AS ?serCount) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?sys ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs) {?GLitem ?belongs ?phase ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Output> AS ?output) {?GLitem ?output ?ser ;}{?ser <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?element ?input ?GLitem} BIND(\"Provider Count\" AS ?type) {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } } GROUP BY ?type";
		consumerCostCreateQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT(?sys)) AS ?sysCount) (COUNT(DISTINCT(?ser)) AS ?serCount) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Consumer> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} BIND(<http://semoss.org/ontologies/Relation/Influences> AS ?influences){?sys ?influences ?GLitem ;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs)  {?GLitem ?belongs ?phase ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Output> AS ?output) {?GLitem ?output ?ser ;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?inputElement ?input ?GLitem} BIND(\"Consumer Count\" AS ?type) {?sys ?OwnedBy <http://health.mil/ontologies/Concept/SystemOwner/Central> } } GROUP BY ?type";
		genericCostCreateQuery = "SELECT DISTINCT ?type (COUNT(DISTINCT(?sys)) AS ?sysCount) (COUNT(DISTINCT(?ser)) AS ?serCount) WHERE { {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs) {?GLitem ?belongs ?phase ;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?inputElement ?input ?GLitem} BIND( <http://semoss.org/ontologies/Concept/GLTag/Generic> AS ?gltag). BIND( <http://semoss.org/ontologies/Relation/Output> AS ?output) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?GLitem ?output ?ser ;}  {?ser <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BIND(\"Generic\" as ?sys) BIND(\"Generic Count\" AS ?type) } GROUP BY ?type";*/
	}
	
	/**
	 * Iterate through a list of systems and use processing to retrieve the binding string for each.
	 * @param 	List of systems.
	
	 * @return 	Binding string. */
	public String getBindingString(String[] sysArray)
	{
		String retString = "";
		for(int i = 0;i < sysArray.length;i++)
		{
			retString = retString + "(\"" + sysArray[i] + "\")";
				
		}
		return retString;
	}
	
}
