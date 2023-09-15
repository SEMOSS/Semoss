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
package prerna.ui.components.specific.ousd;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.annotations.BREAKOUT;
import prerna.ui.components.specific.tap.SimilarityFunctions;
import prerna.ui.components.specific.tap.SimilarityHeatMapSheet;
import prerna.util.Utility;

@BREAKOUT
public class OUSDSysSimHeatMapSheet extends SimilarityHeatMapSheet{
	boolean createSystemBindings = true;
	String systemListBindings = "BINDINGS ?System ";
	
	ArrayList<Object[]> list;
	String[] names;
	
	@Override
	public ArrayList<Object[]> getList() {
		return this.list;
	}
	
	/**
	 * Constructor for SysSimeHeatMapSheet.
	 */
	public OUSDSysSimHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		setComparisonObjectTypes("System1", "System2");
	}

//	/**
//	 * Adds the health grid, refresh, and bar chart listeners when the navigation has finished.
//	 */
//	@Override
//	public void registerFunctions()
//	{
//		super.registerFunctions();
//		SysSimHealthGridListener healthGridCall = new SysSimHealthGridListener();
//	   	browser.registerFunction("healthGrid",  healthGridCall);    	
//	}
	
	@Override
	public void createView() {
		if (!(this.query).equals("NULL") || this.query.isEmpty()) {			
			if (list!=null && list.isEmpty()) {
				return;
			}
			super.createView();
		}
		else if ((this.query).equals("NULL")) {
			super.createView();
		}
	}
	
	@Override
	public void createData()
	{
		if (!(this.query).equals("NULL") || this.query.isEmpty()) {
			super.createData();
			if (list!=null && list.isEmpty()) {
				Utility.showError("Query returned no results.");
				return;
			}
		}
		SimilarityFunctions sdf = new SimilarityFunctions();
		if(this.pane!=null)
			addPanel();
		// this would be create the data
//		Hashtable dataBLUCompleteHash = new Hashtable();
//		Hashtable overallHash;
		//get list of systems first
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		String defaultSystemsQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>}{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner}} BINDINGS ?SystemOwner {(<http://semoss.org/ontologies/Concept/SystemOwner/DFAS>)}";
		defaultSystemsQuery = addBindings(defaultSystemsQuery);
		comparisonObjectList = sdf.createComparisonObjectList(this.engine.getEngineId(), defaultSystemsQuery);
		sdf.setComparisonObjectList(comparisonObjectList);
		
		//data provided by system
		updateProgressBar("20%...Evaluating Data created by systems Score", 20);
		String dataProvidedQuery = "SELECT DISTINCT ?System ?Data WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?Data}{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>}{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner}} BINDINGS ?SystemOwner {(<http://semoss.org/ontologies/Concept/SystemOwner/DFAS>)}";
		dataProvidedQuery = addBindings(dataProvidedQuery);
		Hashtable dataProvidedHash = sdf.compareObjectParameterScore(this.engine.getEngineId(), dataProvidedQuery, SimilarityFunctions.VALUE);
		dataProvidedHash = processHashForCharting(dataProvidedHash);
		
		//dataDown
//		updateProgressBar("20%...Evaluating Data Downstream Score", 20);
//		String dataDownQuery = "SELECT DISTINCT ?System ?Data WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd1}{?icd1 <http://semoss.org/ontologies/Relation/Payload> ?Data}}";
//		dataDownQuery = addBindings(dataDownQuery);
//		Hashtable dataDownHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), dataDownQuery, SimilarityFunctions.VALUE);
//		dataDownHash = processHashForCharting(dataDownHash);
//		
		//dataUp
//		updateProgressBar("40%...Evaluating Data Upstream Score", 40);
//		String dataUpQuery = "SELECT DISTINCT ?System ?Data WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>;}{?icd1 <http://semoss.org/ontologies/Relation/Consume> ?System}{?icd1 <http://semoss.org/ontologies/Relation/Payload> ?Data}}";
//		dataUpQuery = addBindings(dataUpQuery);
//		Hashtable dataUpHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), dataUpQuery, SimilarityFunctions.VALUE);
//		dataUpHash = processHashForCharting(dataUpHash);

		updateProgressBar("30%...Evaluating BLU Score", 30);
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU }{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>}{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner}} BINDINGS ?SystemOwner {(<http://semoss.org/ontologies/Concept/SystemOwner/DFAS>)}";
		bluQuery = addBindings(bluQuery);
		Hashtable bluHash = sdf.compareObjectParameterScore(this.engine.getEngineId(), bluQuery, SimilarityFunctions.VALUE);
		bluHash = processHashForCharting(bluHash);
		
		
		updateProgressBar("50%...Evaluating System Supporting Activity", 50);
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?Activity}{?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>}{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner}} BINDINGS ?SystemOwner {(<http://semoss.org/ontologies/Concept/SystemOwner/DFAS>)}";
		actQuery = addBindings(actQuery);
		Hashtable actHash = sdf.compareObjectParameterScore(this.engine.getEngineId(), actQuery, SimilarityFunctions.VALUE);
		actHash = processHashForCharting(actHash);

		updateProgressBar("60%...Evaluating System Supporting Functional Strategies", 60);
		String fsQuery ="SELECT DISTINCT ?System ?FunctionalStrategyInitiative Where{{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?FunctionalStrategyInitiative <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FunctionalStrategyInitiative>} {?System <http://semoss.org/ontologies/Relation/Provide> ?FunctionalStrategyInitiative} {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>}{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner}} BINDINGS ?SystemOwner {(<http://semoss.org/ontologies/Concept/SystemOwner/DFAS>)}";
		fsQuery = addBindings(fsQuery);
		Hashtable fsHash = sdf.compareObjectParameterScore(this.engine.getEngineId(), fsQuery, SimilarityFunctions.VALUE);
		fsHash = processHashForCharting(fsHash);

		ArrayList<Hashtable> hashArray = new ArrayList<Hashtable>();
		
		boolean allQueriesAreEmpty = true;
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		if (dataProvidedHash != null && !dataProvidedHash.isEmpty()) {
			paramDataHash.put("Data_Provided", dataProvidedHash);
			allQueriesAreEmpty = false;
		}
		if (bluHash != null && !bluHash.isEmpty()) {
			paramDataHash.put("BLU_Provided", bluHash);
			allQueriesAreEmpty = false;
		}
		if (actHash != null && !actHash.isEmpty()) {
			paramDataHash.put("Activities_Supported", actHash);
			allQueriesAreEmpty = false;
		}
		if (fsHash != null && !fsHash.isEmpty()) {
			paramDataHash.put("Functional_Strategy_Provided", fsHash);
			allQueriesAreEmpty = false;
		}
		
		if (allQueriesAreEmpty == true) {
			Utility.showError("System Similarity Heat Map returned no results.");
			return;
		}
		
		//allHash.put("dataSeries", testDataHash);
		allHash.put("title",  "System Similarity");
		allHash.put("xAxisTitle", comparisonObjectTypeX);
		allHash.put("yAxisTitle", comparisonObjectTypeY);
		allHash.put("value", "Score");
		allHash.put("sysDup", true);

	}
	
	public String addBindings(String sysSimQuery) {
		
		String defaultBindings = "BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		
		
		//If a query is not specified, append the default SystemUser bindings
		if ((this.query).equals("NULL") || (this.query).equals("null") || (this.query).equals("Null") || this.query == null) {
//			sysSimQuery = sysSimQuery + defaultBindings;
		}
		//if a query is specified, bind the system list to the system similarity query.
		else {
			//only create the bindings string once
			if (createSystemBindings == true) {
				String systemURIs = "{";
				for( int i = 0; i < list.size(); i++) {
					Object[] values = list.get(i);
					String system = "";
					for (Object systemResult : values) {
						system = "(<" + systemResult.toString() + ">)";
					}
					systemURIs = systemURIs + system;
				}
				systemURIs = systemURIs + "}";
				systemListBindings = systemListBindings + systemURIs;
				createSystemBindings = false;
			}
			sysSimQuery = sysSimQuery + systemListBindings;
		}
		
		return sysSimQuery;		
	}
	
	/*@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getRawVar(varName);
	}*/

	@Override
	public Hashtable getDataMakerOutput(String... selectors) {
		List args = prepareOrderedVars();
		Map testHash = new Hashtable();
		List<Map<String, Map<String, Double>>> list = calculateHash(args, testHash);
		//this.paramDataHash.clear();
//		for(Hashtable hash : list){ //Browser crashes on TAP Core when sending all of the data... for now just going to add one array (max of 20,000 cells)
//			specdataHash.putAll(list.get(0));
//		}
		/*Hashtable dimHash = new Hashtable();
		Enumeration enumKey = allHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) allHash.get(key);
			
			dimHash.put(value,key);
			//System.out.println("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
		}*/
		dataHash = new Hashtable();
//		dataHash.put("dimHash", dimHash);
		dataHash.put("dimData", args);
//		dataHash.put("dataSeries", specdataHash);
		dataHash.put("xAxisTitle", comparisonObjectTypeX);
		dataHash.put("yAxisTitle", comparisonObjectTypeY);
		dataHash.put("value", "Score");
		dataHash.put("sysDup", false); // TODO: does this work? for getting rid of sys app health grid
		Hashtable returnHash = (Hashtable) super.getDataMakerOutput();
		if (dataHash != null)
			returnHash.put("specificData", dataHash);
		List<Object[]> tableData = flattenData(list, true);
		returnHash.put("data", tableData);
		returnHash.put("headers", new String[]{comparisonObjectTypeX,comparisonObjectTypeY,"Score"});
//		Gson gson = new Gson();
//		logger.info("Converted " + gson.toJson(dataHash));
//		logger.info("Converted gson");
		
		return returnHash;
	}
	
	public List<Object[]> flattenData(List<Map<String, Map<String, Double>>> list, boolean initialLoad)
	{
		logger.info("Starting to flatten data");
		ArrayList<Object[]> returnTable = new ArrayList<Object[]>();
		for(Map<String, Map<String, Double>> hash : list){
//		Hashtable<String,Hashtable<String, Double>> hash = list.get(0);
			Collection<String> keys = hash.keySet();
			Collection<String> newKeys = new ArrayList<String>(); // avoid concurrent modification
			newKeys.addAll(keys);
			Iterator<String> it = newKeys.iterator();
			while(it.hasNext()){
				String key = it.next();
				Map value = hash.get(key);
				Double score = (Double) value.get("Score");
//				if(score > 50){
					Object[] row = new Object[3];
					row[0] = value.get(comparisonObjectTypeX);
					row[1] = value.get(comparisonObjectTypeY);
					row[2] = score;//
					returnTable.add(row);
//				}
//				else{ 
//					if (initialLoad) {    // remove it so we don't store unnecessary data on the back-end
//						hash.remove(key);
//						clearParamDataHash(key);
//					}
//				}
			}
			logger.info("done flattening one hash");
		}
		logger.info("done flattening");
		return returnTable;		
	}
	
	public void clearParamDataHash(String key) {
		Collection<String> keys = paramDataHash.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext()){
			String paramKey = it.next();
			paramDataHash.get(paramKey).remove(key);
		}
	}
	
	//web function for refreshing the heat map data given parameters
	public Map refreshSysSimData(Hashtable webDataHash) {
		Gson gson = new Gson();
		Map retHash = new Hashtable();
		List<String> selectedVarsList = (List<String>) webDataHash.get("selectedVars");
		Map<String, Double> specifiedWeights = gson.fromJson(gson.toJson(webDataHash.get("specifiedWeights")), new TypeToken<Map<String, Double>>() {}.getType());
		List<Map<String, Map<String, Double>>> calculatedHash = this.calculateHash(selectedVarsList, specifiedWeights);
		List<Object[]> table = this.flattenData(calculatedHash, false);
		retHash.put("data", table);
		return retHash;
	}
	
	//web function for bar chart data
	public Map getSysSimBarData(Hashtable webDataHash) {
		Gson gson = new Gson();
		Map retHash = new Hashtable();
		List<String> categoryArray = (List<String>) webDataHash.get("categoryArray");
		Map<String, Double> thresh = gson.fromJson(gson.toJson(webDataHash.get("thresh")), new TypeToken<Map<String, Double>>() {}.getType());
		String cellKey = (String) webDataHash.get("cellKey");
		retHash.put("barData", this.getSimBarChartData(cellKey, categoryArray, thresh));
		return retHash;
	}

}
