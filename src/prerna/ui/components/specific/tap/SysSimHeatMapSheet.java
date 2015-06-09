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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import org.openrdf.model.Literal;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.ISelectStatement;
import prerna.ui.main.listener.specific.tap.SysSimHealthGridListener;
import prerna.util.Utility;


/**
 */
public class SysSimHeatMapSheet extends SimilarityHeatMapSheet{
	boolean createSystemBindings = true;
	String systemListBindings = "BINDINGS ?System ";
	
	/**
	 * Constructor for SysSimeHeatMapSheet.
	 */
	public SysSimHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		setComparisonObjectTypes("System1", "System2");
	}

	/**
	 * Adds the health grid, refresh, and bar chart listeners when the navigation has finished.
	 */
	@Override
	public void registerFunctions()
	{
		super.registerFunctions();
		SysSimHealthGridListener healthGridCall = new SysSimHealthGridListener();
	   	browser.registerFunction("healthGrid",  healthGridCall);    	
	}
	
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
		Hashtable dataBLUCompleteHash = new Hashtable();
//		Hashtable overallHash;
		//get list of systems first
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		String defaultSystemsQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}}";
		defaultSystemsQuery = addBindings(defaultSystemsQuery);
		comparisonObjectList = sdf.createComparisonObjectList(this.engine.getEngineName(), defaultSystemsQuery);
		sdf.setComparisonObjectList(comparisonObjectList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?Provide ?Data .}{?System ?UsedBy ?SystemUser}}";
		dataQuery = addBindings(dataQuery);
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU }{?System ?UsedBy ?SystemUser}}";
		bluQuery = addBindings(bluQuery);
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(this.engine.getEngineName(), dataQuery, bluQuery, SimilarityFunctions.VALUE);
		dataBLUCompleteHash = processHashForCharting(dataBLUHash);
		
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}{?System ?UsedBy ?SystemUser}}";
		theaterQuery = addBindings(theaterQuery);
		updateProgressBar("30%...Evaluating Deployment Score", 30);
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(this.engine.getEngineName(), theaterQuery, "Theater", "Garrison", "Both");
		theaterHash = processHashForCharting(theaterHash);
		//dataHash = processOverallScore(dataHash, theaterHash);
		
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}{?System ?UsedBy ?SystemUser}}";
		dwQuery = addBindings(dwQuery);
		updateProgressBar("40%...Evaluating System Transactional Score", 40);
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(this.engine.getEngineName(), dwQuery, "'Yes'", "'No'", "Both");
		dwHash = processHashForCharting(dwHash);
		//dataHash = processOverallScore(dataHash, dwHash);
		
		//BP
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?BusinessProcess}{?System ?UsedBy ?SystemUser}}";
		bpQuery = addBindings(bpQuery);
		updateProgressBar("50%...Evaluating System Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), bpQuery, SimilarityFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?Activity}{?System ?UsedBy ?SystemUser}}";
		actQuery = addBindings(actQuery);
		updateProgressBar("55%...Evaluating System Supporting Activity", 55);
		Hashtable actHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), actQuery, SimilarityFunctions.VALUE);
		actHash = processHashForCharting(actHash);
		
		String userQuery ="SELECT DISTINCT ?System ?Personnel WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel> ;} {?System <http://semoss.org/ontologies/Relation/UsedBy> ?Personnel}{?System ?UsedBy ?SystemUser}}";
		userQuery = addBindings(userQuery);
		updateProgressBar("60%...Evaluating System Users", 60);
		Hashtable userHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), userQuery, SimilarityFunctions.VALUE);
		userHash = processHashForCharting(userHash);
		
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System <http://semoss.org/ontologies/Relation/Utilizes> ?UserInterface}{?System ?UsedBy ?SystemUser}}";
		uiQuery = addBindings(uiQuery);
		updateProgressBar("70%...Evaluating User Interface", 70);
		Hashtable uiHash = sdf.compareObjectParameterScore(this.engine.getEngineName(), uiQuery, SimilarityFunctions.VALUE);
		uiHash = processHashForCharting(uiHash);
		
		ArrayList<Hashtable> hashArray = new ArrayList<Hashtable>();
		//hashArray.add(bpHash);
		//hashArray.add(actHash);
		//hashArray.add(userHash);
		//dataHash = processOverallScoreByAverage(dataHash,hashArray);
		
		boolean allQueriesAreEmpty = true;
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		if (bpHash != null && !bpHash.isEmpty()) {
			paramDataHash.put("Business_Processes_Supported", bpHash);
			allQueriesAreEmpty = false;
		}
		if (actHash != null && !actHash.isEmpty()) {
			paramDataHash.put("Activities_Supported", actHash);
			allQueriesAreEmpty = false;
		}
		if (dataBLUCompleteHash != null && !dataBLUCompleteHash.isEmpty()) {
			paramDataHash.put("Data_and_Business_Logic_Supported", dataBLUCompleteHash);
			allQueriesAreEmpty = false;
		}
		if (theaterHash != null && !theaterHash.isEmpty()) {
			paramDataHash.put("Deployment_(Theater/Garrison)",  theaterHash);
			allQueriesAreEmpty = false;
		}
		if (dwHash != null && !dwHash.isEmpty()) {
			paramDataHash.put("Transactional_(Yes/No)", dwHash);
			allQueriesAreEmpty = false;
		}
		if (userHash != null && !userHash.isEmpty()) {
			paramDataHash.put("User_Types", userHash);
			allQueriesAreEmpty = false;
		}
		if (uiHash != null && !uiHash.isEmpty()) {
			paramDataHash.put("User_Interface_Types_(PC/Mobile/etc.)", uiHash);
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
		
		
		//If a query is not specifed, append the default SystemUser bindings
		if ((this.query).equals("NULL") || (this.query).equals("null") || (this.query).equals("Null") || this.query == null) {
			sysSimQuery = sysSimQuery + defaultBindings;
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
	public Object getData() {
		ArrayList args = prepareOrderedVars();
		Hashtable testHash = new Hashtable();
		ArrayList<Hashtable<String, Hashtable<String, Double>>> list = calculateHash(args, testHash);
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
		dataHash.put("sysDup", true);
		Hashtable returnHash = (Hashtable) super.getData();
		if (dataHash != null)
			returnHash.put("specificData", dataHash);
		ArrayList<Object[]> tableData = flattenData(list, true);
		returnHash.put("data", tableData);
		returnHash.put("headers", new String[]{comparisonObjectTypeX,comparisonObjectTypeY,"Score"});
//		Gson gson = new Gson();
//		logger.info("Converted " + gson.toJson(dataHash));
//		logger.info("Converted gson");
		
		return returnHash;
	}
	
	public ArrayList<Object[]> flattenData(ArrayList<Hashtable<String, Hashtable<String, Double>>> list, boolean initialLoad)
	{
		logger.info("Starting to flatten data");
		ArrayList<Object[]> returnTable = new ArrayList<Object[]>();
		for(Hashtable<String,Hashtable<String, Double>> hash : list){
//		Hashtable<String,Hashtable<String, Double>> hash = list.get(0);
			Collection<String> keys = hash.keySet();
			Collection<String> newKeys = new ArrayList<String>(); // avoid concurrent modification
			newKeys.addAll(keys);
			Iterator<String> it = newKeys.iterator();
			while(it.hasNext()){
				String key = it.next();
				Hashtable value = hash.get(key);
				Double score = (Double) value.get("Score");
				if(score > 50){
					Object[] row = new Object[3];
					row[0] = value.get(comparisonObjectTypeX);
					row[1] = value.get(comparisonObjectTypeY);
					row[2] = score;//
					returnTable.add(row);
				}
				else{ 
					if (initialLoad) {    // remove it so we don't store unnecessary data on the back-end
						hash.remove(key);
						clearParamDataHash(key);
					}
				}
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
	
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		Object var = sjss.getRawVar(varName);
			if( var != null && var instanceof Literal) {
				var = sjss.getVar(varName);
			} 
		return var;
	}
	
	//web function for refreshing the heat map data given parameters
	public Hashtable refreshSysSimData(Hashtable webDataHash) {
		Gson gson = new Gson();
		Hashtable retHash = new Hashtable();
		ArrayList<String> selectedVarsList = (ArrayList<String>) webDataHash.get("selectedVars");
		Hashtable<String, Double> specifiedWeights = gson.fromJson(gson.toJson(webDataHash.get("specifiedWeights")), new TypeToken<Hashtable<String, Double>>() {}.getType());
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedHash = this.calculateHash(selectedVarsList, specifiedWeights);
		ArrayList<Object[]> table = this.flattenData(calculatedHash, false);
		retHash.put("data", table);
		return retHash;
	}
	
	//web function for bar chart data
	public Hashtable getSysSimBarData(Hashtable webDataHash) {
		Gson gson = new Gson();
		Hashtable retHash = new Hashtable();
		ArrayList<String> categoryArray = (ArrayList<String>) webDataHash.get("categoryArray");
		Hashtable<String, Double> thresh = gson.fromJson(gson.toJson(webDataHash.get("thresh")), new TypeToken<Hashtable<String, Double>>() {}.getType());
		String cellKey = (String) webDataHash.get("cellKey");
		retHash.put("barData", this.getSimBarChartData(cellKey, categoryArray, thresh));
		return retHash;
	}

}
