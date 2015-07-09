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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.swing.event.ListSelectionEvent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Utility;

public class SequencingDecommissioningPlaySheet extends GridPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());
//	Map dataHash = new Hashtable();

//	String INTERFACE_QUERY = "SELECT DISTINCT ?System2 ?System3 WHERE { {?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payloads>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Interface_Control_Document>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provides>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consumes>;} {?System2 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;} } BINDINGS ?Data1 {@Data-Data@}";
	
	// Add bindings to below queries before using
	// Query 1) SELECT DISTINCT ?Activity ?BLU WHERE { {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?BLU a <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU} }
	// Query 2) SELECT DISTINCT ?Activity ?BLU ?System WHERE { {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?BLU a <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Activity <http://semoss.org/ontologies/Relation/Needs> ?BLU}{?System <http://semoss.org/ontologies/Relation/Performs> ?BLU} }
	List<String> compObjList;

	String depQuery;
	String compObjQuery;
	String depObjFilterString;
	String depObjQuery;
	List<String> addtlQueries;

	public SequencingDecommissioningPlaySheet() {
		super();
	}

	@Override
	public void createData() {
		// populate data object list with list of all data objects (query should be passed in)
		ISelectWrapper depObjWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.depObjQuery);
		//wrapper manager, etc. to fill data obj list
		String[] depObjNames = depObjWrap.getVariables();
		depObjFilterString = "<"+depObjWrap.next().getRawVar(depObjNames[0])+">";
		while(depObjWrap.hasNext()){
			String thisDepObj = depObjWrap.next().getRawVar(depObjNames[0])+"";
			depObjFilterString = depObjFilterString + ", <"+thisDepObj + ">";
		}

		// populate system list with list of all systems (query should also be passed in)
		this.compObjList = new ArrayList<String>();
		ISelectWrapper compObjWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.compObjQuery);
		//wrapper manager, etc. to fill sys list
		String[] compObjNames = compObjWrap.getVariables();
		String compObjName = compObjNames[0];
		while(compObjWrap.hasNext()){
			this.compObjList.add(compObjWrap.next().getRawVar(compObjName)+"");
		}

		Integer[][] dependMatrix = createDependencyMatrices();
		HashMap<Integer, List<ArrayList<Integer>>> groups = createDecommissioningGroups(dependMatrix);
		
//		List<HashMap<String, List<Object[]>>> addtlCols = createAddtlCols(groups);
		
		createTable(groups, compObjName);
	}
	
	private void createTable(HashMap<Integer, List<ArrayList<Integer>>> groups, String compObjName){
		int groupCounter = 0;

		// build the names
		List<String> namesList = new ArrayList<String>();
		namesList.add(compObjName);
		namesList.add(compObjName + " Group");

		list = new ArrayList<Object[]>();
		//key is counter for one level above group
		for(Integer key: groups.keySet()){
			List<ArrayList<Integer>> depGroups = groups.get(key);
			for(ArrayList<Integer> depGroup: depGroups){
				String keyToGroupCounter = new String(key.toString()+"."+groupCounter);
//				double location = Double.parseDouble(keyToGroupCounter);
				
				List<Object[]> addtlCols = new ArrayList<Object[]>();
				for(String addtlQuery: this.addtlQueries){
					processQuery(depGroup, addtlQuery, addtlCols, namesList);
				}
				for(Integer compObj: depGroup){
					Object[] depObj = new Object[namesList.size()];
					depObj[0] = Utility.getInstanceName(compObjList.get(compObj));
					depObj[1] = keyToGroupCounter;
					if(addtlCols.isEmpty()){
						list.add(depObj);
					}
					else {
						for(Object[] row : addtlCols){
							Object[] depObj2 = depObj.clone();
							for(int addtlIdx = 2 ; addtlIdx < row.length; addtlIdx++ ){
								LOGGER.debug(row[addtlIdx]);
								depObj2[addtlIdx] = row[addtlIdx];
							}
							list.add(depObj2);
						}
					}
					LOGGER.debug("Added object "+compObj);
				}
				groupCounter++;
			}
			groupCounter = 0;
		}

		// set the names
		this.names = new String[namesList.size()];
		this.names = namesList.toArray(this.names);
	}
	
	private void processQuery(List<Integer> depGroup, String addtlQuery, List<Object[]> addtlCols, List<String> namesList){
		String bindings = "";
		for(Integer compObj: depGroup){
			String name = compObjList.get(compObj);
			bindings = bindings + "(<" + name + ">)";
		}
		String filledQuery = addtlQuery.replaceAll("~~GroupMembers-GroupMembers~~", bindings).replaceAll("~~DepObj-DepObj~~", this.depObjFilterString);
		LOGGER.debug("col query " + filledQuery);
		ISelectWrapper sw = WrapperManager.getInstance().getSWrapper(this.engine, filledQuery);
		
		String[] wrapNames = sw.getVariables();
		Integer[] wrapIdx = new Integer[wrapNames.length]; // find if the names already exist as col headers. if they do, get the idx. if not, add and get the idx
		for(int nIdx = 0; nIdx < wrapNames.length; nIdx++){
			String name = wrapNames[nIdx];
			if(!namesList.contains(name)){
				namesList.add(name);
			}
			wrapIdx[nIdx] = namesList.indexOf(name);
		}
		
		// process the query
		while(sw.hasNext()){
			Object[] row = new Object[namesList.size()];
			ISelectStatement ss = sw.next();
			for(int i = 0; i<wrapIdx.length; i++){
				row[wrapIdx[i]] = ss.getRawVar(wrapNames[i]); 
			}
			addtlCols.add(row);
		}
		
		return;
	}

	private Integer[][] createDependencyMatrices(){
		// iterate through list and for each:
		// create the data network of systems
		// iterate down through the network, capturing :
		//   1. the level that the system is on wrt that data object
		//   2. the system - system dependencies
		// (is it possible that we have islands? can a system exist outside of the main network?)

		Integer[][] dependMatrix = new Integer[compObjList.size()][compObjList.size()];
		String query = this.depQuery.replace("~~DepObj-DepObj~~", depObjFilterString);
		
		LOGGER.info("Final dependency query :  " + query);
		ISelectWrapper depWrap = WrapperManager.getInstance().getSWrapper(this.engine, query);
		//wrapper manager, etc. to fill sys list
		String[] depNames = depWrap.getVariables();
		while(depWrap.hasNext()){
			ISelectStatement ss = depWrap.next();
			String compObj1 = ss.getRawVar(depNames[0])+"";
			String compObj2 = ss.getRawVar(depNames[1])+"";
			LOGGER.debug("Found relation: " + compObj1 + " depends on " + compObj2);

			int compObj1loc = this.compObjList.indexOf(compObj1);
			int compObj2loc = this.compObjList.indexOf(compObj2);
			LOGGER.debug("Found location: " + compObj1loc + " and " + compObj2loc);

			if(compObj1loc >= 0 && compObj2loc >= 0){
				dependMatrix[compObj1loc][compObj2loc] = 1;
			}
			else{
				LOGGER.error("Exluding systems " + compObj1 + " and " + compObj2);
			}
		}

		return dependMatrix;
	}

	/**
	 * @param systemMatrix
	 */
	private static HashMap<Integer, List<ArrayList<Integer>>> createDecommissioningGroups(Integer[][] dependMatrix)
	{

		//retrieve matrices
		List<Integer> procRows = new ArrayList<Integer>();
		HashMap<Integer, Integer> rowSums = new HashMap<Integer, Integer>();
		HashMap<Integer, List<ArrayList<Integer>>> groups = new HashMap<Integer, List<ArrayList<Integer>>>();
		int groupNumber = 0;

		//add all rows into map
		for(int i=0; i < dependMatrix.length; i++){
			int rowTotal = 0;
			for(int j=0; j < dependMatrix[i].length; j++){
				if(dependMatrix[i][j]==null){
					continue;
				}
				rowTotal = rowTotal + dependMatrix[i][j];
			}
			LOGGER.debug("Mapping row "+i+" to total "+rowTotal);
			rowSums.put(i, rowTotal);
		}

		//loop until every system has been processed
		while(procRows.size() != dependMatrix.length){
			//find next group
			List<Integer> currentGroup = findNextGroup(rowSums, procRows, dependMatrix);

			//recursive method through group and assign group number
			List<ArrayList<Integer>> group = new ArrayList<ArrayList<Integer>>();
			for(Integer row: currentGroup){
				LOGGER.debug("Adding row "+row+" to processed list");
				ArrayList<Integer> systemRow = new ArrayList<Integer>();
				assembleGroup(row, procRows, dependMatrix, systemRow);
				if(!systemRow.isEmpty()){
					group.add(systemRow);
				}
			}
			groups.put(groupNumber, group);
			groupNumber++;

			//update map
			rowSums = updateRowTotals(rowSums, procRows, dependMatrix);
			LOGGER.info("-----------------------");
		}

		for(Integer key: groups.keySet()){
			LOGGER.info("Group is "+key+". Groups are: ");
			LOGGER.info(groups.get(key));
		}

		return groups;
	}

	/**
	 * @param rowMap
	 * @param procRows
	 * @param systemMatrix
	 * @return
	 */
	private static List<Integer> findNextGroup(HashMap<Integer, Integer> rowMap, List<Integer> procRows, Integer[][] systemMatrix){
		//finds lowest row total
		List<Integer> lowGroup = new ArrayList<Integer>();
		int currentLow = Integer.MAX_VALUE;
		for(Integer key: rowMap.keySet()){
			if(!procRows.contains(key)){
				if(rowMap.get(key) < currentLow){
					currentLow = rowMap.get(key);
					LOGGER.debug("New low total ("+currentLow+") for row "+key+".");
					lowGroup.clear();
					lowGroup.add(key);
				}else if(rowMap.get(key) == currentLow){
					LOGGER.debug("Matching low found for row "+key+".");
					lowGroup.add(key);
				}
			}else{
				continue;
			}
		}
		LOGGER.info("New lows: "+lowGroup);
		return lowGroup;
	}

	/**
	 * @param rowTotals
	 * @param procRows
	 * @param systemMatrix
	 * @return
	 */
	private static HashMap<Integer, Integer> updateRowTotals(HashMap<Integer, Integer> rowTotals, List<Integer> procRows, Integer[][] systemMatrix){
		for(Integer key: rowTotals.keySet()){
			int newTotal = 0;
			LOGGER.debug("Updating row "+key);
			for(int j=0; j < systemMatrix[key].length; j++){
				if(systemMatrix[key][j] != null && !procRows.contains(j)){
					LOGGER.debug("Added system "+j+" to total for row "+key);
					newTotal = newTotal + systemMatrix[key][j];
					rowTotals.put(key, newTotal);
				}else if(procRows.contains(j)){
					LOGGER.debug("Row "+j+" was already processed. Skipping row");
				}
				LOGGER.debug("New total for row "+key+" is "+newTotal);
			}
		}
		return rowTotals;
	}

	/**
	 * @param row
	 * @param procRows
	 * @param dependMatrix
	 * @param rowArray
	 */
	private static void assembleGroup(Integer row, List<Integer> procRows, Integer[][] dependMatrix, ArrayList<Integer> rowArray){

		//add row first time
		if(procRows.contains(row)){
			return;
		}else{
			procRows.add(row);
			if(rowArray.contains(row)){
				System.err.println("this is strange... adding "+ row +" again to " + rowArray);
			}
			LOGGER.debug("Marked row "+row);
			rowArray.add(row);

			//mark row as processed. removes possible loops
			//find all dependencies of row
			LOGGER.info("Finding dependencies of row "+row);
			for(int j=0; j < dependMatrix[row].length; j++){
				if(dependMatrix[row][j] != null && dependMatrix[row][j] == 1 && !procRows.contains(j)){
					LOGGER.debug("System "+j+" depends on "+row);
					assembleGroup(j, procRows, dependMatrix, rowArray);
				}
			}
		}

		LOGGER.debug(rowArray);
		return;
	}

	@Override
	public void setQuery(String query) {

		String[] items = query.split("\\+{3}");

		LOGGER.info("Setting comparison object query as " + items[0]);
		this.compObjQuery = items[0];

		LOGGER.info("Setting dependency objects query as " + items[1]);
		this.depObjQuery = items[1];

		LOGGER.info("Setting dependency query as " + items[2]);
		this.depQuery = items[2];

		addtlQueries = new ArrayList<String>();
		for (int qIdx = 3; qIdx < items.length; qIdx ++ ){
			addtlQueries.add(items[qIdx]);
		}
	}

	//	@Override
	//	public Object getData() {
	//		Hashtable returnHash = (Hashtable) super.getData();
	//		if (dataHash != null)
	//			returnHash.put("specificData", dataHash);
	//		return returnHash;
	//	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// once we determine display, we will use this to align data to display
		return null;
	}

	public static void main(String args[]){

		PropertyConfigurator.configure("log4j.prop");

		Integer[][] testSystems = new Integer [5][5];

		testSystems[0][0] = null;
		testSystems[0][1] = 1;
		testSystems[0][2] = null;
		testSystems[0][3] = null;
		testSystems[0][4] = null;
		testSystems[1][0] = 1;
		testSystems[1][1] = null;
		testSystems[1][2] = null;
		testSystems[1][3] = null;
		testSystems[1][4] = null;
		testSystems[2][0] = null;
		testSystems[2][1] = null;
		testSystems[2][2] = null;
		testSystems[2][3] = null;
		testSystems[2][4] = null;
		testSystems[3][0] = null;
		testSystems[3][1] = null;
		testSystems[3][2] = null;
		testSystems[3][3] = null;
		testSystems[3][4] = 1;
		testSystems[4][0] = null;
		testSystems[4][1] = null;
		testSystems[4][2] = null;
		testSystems[4][3] = 1;
		testSystems[4][4] = null;

		HashMap<Integer, List<ArrayList<Integer>>> groups = createDecommissioningGroups(testSystems);
		ArrayList <Object []> list = new ArrayList<Object[]>();
		int groupCounter = 0;
		
		//key is counter for one level above group
		for(Integer key: groups.keySet()){
			for(ArrayList<Integer> depGroup: groups.get(key)){
				for(Integer compObj: depGroup){
					String keyToGroupCounter = new String(key.toString()+"."+groupCounter);
					double location = Double.parseDouble(keyToGroupCounter);
					Object[] depObj = new Object[]{compObj, location};
					list.add(depObj);
					LOGGER.info("Added ["+depObj[0]+", "+depObj[1]+"]");
				}
				groupCounter++;
			}
			groupCounter = 0;
		}
		
	}

}