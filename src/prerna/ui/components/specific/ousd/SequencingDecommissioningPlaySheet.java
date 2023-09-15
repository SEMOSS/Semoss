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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.annotations.BREAKOUT;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.PlaySheetRDFMapBasedEnum;
import prerna.util.Utility;

@BREAKOUT
public class SequencingDecommissioningPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(SequencingDecommissioningPlaySheet.class);
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
	String compObjName;
	List<String> addtlQueries;
	Map<String, List<String>> dependMap;

	public SequencingDecommissioningPlaySheet() {
		super();
	}

	public Map<Integer, List<List<Integer>>> collectData(){
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
		this.compObjList = new ArrayList<>();
		ISelectWrapper compObjWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.compObjQuery);
		//wrapper manager, etc. to fill sys list
		String[] compObjNames = compObjWrap.getVariables();
		this.compObjName = compObjNames[0];
		while(compObjWrap.hasNext()){
			this.compObjList.add(compObjWrap.next().getRawVar(compObjName)+"");
		}
		Collections.sort(this.compObjList);

		this.dependMap = new HashMap<>();

		Integer[][] dependMatrix = createDependencyMatrices();
		List<List<Integer>> groups = createGroups(dependMatrix);
		Map<Integer, List<List<Integer>>> decomGroups = createDecommissioningGroups(groups, this.dependMap);
		return decomGroups;
	}

	@Override
	public void createData() {

		//		List<HashMap<String, List<Object[]>>> addtlCols = createAddtlCols(groups);
		Map<Integer, List<List<Integer>>> decomGroups = collectData();
		Object[] res = getResults(decomGroups);
		createTable((Map<String, List<String>>)res[0], (Map<String, List<String>>)res[1], (List<String>)res[2], this.compObjName);
	}

	private static Map<Integer, List<List<Integer>>> createDecommissioningGroups(List<List<Integer>> groups, Map<String, List<String>> dependMap){
		Map<Integer, List<List<Integer>>> decomGroups = new HashMap<>();

		int waveInt = 0;
		while (!groups.isEmpty()){
			logger.info("Begining processing of wave ::::::::::::::::: " + waveInt);
			List<List<Integer>> wave = new ArrayList<>();
			groupsFor: for (List<Integer> group : groups){
				List<List<Integer>> overlapping = new ArrayList<>();
				//				group.removeAll(procRows);
				logger.info("Evaluating for wave " + waveInt + " the group " + Utility.cleanLogString(Arrays.toString(group.toArray())));
				for(List<Integer> waveGroup : wave){
					if(!Collections.disjoint(group, waveGroup)){ // this mean my group has some overlap with a wave group
						overlapping.add(waveGroup);
					}
				}
				if(overlapping.isEmpty()){
					logger.info("NO OVERLAP ::: Adding to wave and continuing");
					wave.add(group);
					continue groupsFor;
				}
				for(List<Integer> overlappingGroup: overlapping){
					if(group.size() > overlappingGroup.size()){
						logger.info("OVERLAP FOUND and not smallest ::: skipping group");
						continue groupsFor;
					}
				}
				// if we get here, we need to remove all overlapping groups from wave array and add our new, smaller group
				logger.info("OVERLAP FOUND and IS smallest ::: removing the following overlapping groups ::: ");
				for(List<Integer> overlappingGroup: overlapping){
					logger.info("removing " + Utility.cleanLogString(Arrays.toString(overlappingGroup.toArray())));
					wave.remove(overlappingGroup);
				}

				wave.add(group);
			}

			logger.info("DONE PROCESSING WAVE ::::::: " + waveInt);
			for(List<Integer> thegroup: wave){
				logger.info("WAVE ::::::: " + waveInt + " :::::::: CONTAINS ::::::::::" + Utility.cleanLogString(thegroup.toString()));
				//				procRows.addAll(thegroup);
				String thegroupsname = new String(waveInt+"."+wave.indexOf(thegroup));
				List<String> thegroupsdependencies = dependMap.get(thegroup);
				if(thegroupsdependencies!=null){
					logger.info("NAMED ::::: " + thegroupsname + " :::::: DEPENDS ON :::::: " + thegroupsdependencies.toString());
				}
				groups.remove(thegroup);
				for(List<Integer> group: groups){
					String groupName = group.toString();
					if(group.removeAll(thegroup)){
						List<String> depends = dependMap.remove(groupName);
						if(depends == null) depends = new ArrayList<String>();
						depends.add(thegroupsname);
						dependMap.put(group.toString(), depends);
					}
				}
			}
			decomGroups.put(waveInt, wave);
			waveInt ++;
		}

		return decomGroups;
	}

	public Object[] getResults(Map<Integer, List<List<Integer>>> groups){
		Map<String, List<String>> group2ActMap = new HashMap<>();
		Map<String, List<String>> group2DependActsMap = new HashMap<>();
		List<String> masterGroupList = new ArrayList<>();

		//key is counter for one level above group
		for(Integer key: groups.keySet()){
			List<List<Integer>> depGroups = groups.get(key);
			for(List<Integer> depGroup: depGroups) {
				String keyToGroupCounter = new String(key.toString()+"."+depGroups.indexOf(depGroup));

				List<String> items = new ArrayList<>();
				for(Integer compObj: depGroup) {
					// add to group2Act array
					String depName = Utility.getInstanceName(compObjList.get(compObj));
					items.add(depName);
				}
				// add array to group2Act map
				group2ActMap.put(keyToGroupCounter, items);

				// add to group2DependActsMap
				List<String> dependsArray = this.dependMap.get(depGroup.toString());
				group2DependActsMap.put(keyToGroupCounter, dependsArray);

				// add to master array
				masterGroupList.add(0, keyToGroupCounter);
			}
		}

		Object[] res = new Object[3];
		res[0] = group2ActMap; // group #s to activities
		res[1] = group2DependActsMap; // group #s to dependecy arrays
		res[2] = masterGroupList; // array of ordered group #s

		return res;
	}

	private void createTable(Map<String, List<String>> group2ActMap, Map<String, List<String>> group2DependActsMap, List<String> masterArray, String compObjName){

		// build the names
		List<String> namesList = new ArrayList<>();
		namesList.add(compObjName);
		namesList.add(compObjName + " Group");
		namesList.add(compObjName + " Group Dependencies");

		//		list = new ArrayList<Object[]>();
		//key is counter for one level above group
		//		for(Integer key: groups.keySet()){
		for(String depGroup: masterArray) {
			//			List<List<Integer>> depGroups = groups.get(key);
			//			for(List<Integer> depGroup: depGroups) {
			//				String keyToGroupCounter = new String(key.toString()+"."+depGroups.indexOf(depGroup));
			//				double location = Double.parseDouble(keyToGroupCounter);
			//				
			List<String> acts = group2ActMap.get(depGroup);
			List<Object[]> addtlCols = new ArrayList<>();
			for(String addtlQuery: this.addtlQueries){
				processQuery(acts, addtlQuery, addtlCols, namesList, compObjName);
			}
			if(this.dataFrame == null){
				String[] names = new String[namesList.size()];
				names = namesList.toArray(names);
				this.dataFrame = new H2Frame(names);
			}
			for(String act: acts) {
				Object[] depObj = new Object[namesList.size()];
				depObj[0] = act;
				depObj[1] = depGroup;
				depObj[2] = group2DependActsMap.get(depGroup);

				if(addtlCols.isEmpty()){
					this.dataFrame.addRow(depObj, namesList.toArray(new String[namesList.size()]));
				}
				else {
					for(Object[] row : addtlCols){
						Object[] depObj2 = depObj.clone();
						for(int addtlIdx = 3 ; addtlIdx < row.length; addtlIdx++ ){
							logger.debug(row[addtlIdx]);
							depObj2[addtlIdx] = row[addtlIdx];
						}
						this.dataFrame.addRow(depObj2, namesList.toArray(new String[namesList.size()]));
					}
				}
				logger.debug("Added object "+depGroup);
			}
			//			}
		}
		// set the names
		//		this.names = new String[namesList.size()];
		//		this.names = namesList.toArray(this.names);
	}

	private void processQuery(List<String> depGroup, String addtlQuery, List<Object[]> addtlCols, List<String> namesList, String compObj){
		String bindings = "";
		for(String name: depGroup){
			bindings = bindings + "(<http://semoss.org/ontologies/Concept/" + compObj + "/"+ name + ">)";
		}
		String filledQuery = addtlQuery.replaceAll("~~GroupMembers-GroupMembers~~", bindings).replaceAll("~~DepObj-DepObj~~", this.depObjFilterString);
		logger.debug("col query " + filledQuery);
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
				row[wrapIdx[i]] = ss.getVar(wrapNames[i]); 
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

		logger.info("Final dependency query :  " + query);
		ISelectWrapper depWrap = WrapperManager.getInstance().getSWrapper(this.engine, query);
		//wrapper manager, etc. to fill sys list
		String[] depNames = depWrap.getVariables();
		while(depWrap.hasNext()){
			ISelectStatement ss = depWrap.next();
			String compObj1 = ss.getRawVar(depNames[0])+"";
			String compObj2 = ss.getRawVar(depNames[1])+"";
			logger.debug("Found relation: " + compObj1 + " depends on " + compObj2);

			int compObj1loc = this.compObjList.indexOf(compObj1);
			int compObj2loc = this.compObjList.indexOf(compObj2);
			logger.debug("Found location: " + compObj1loc + " and " + compObj2loc);

			if(compObj1loc >= 0 && compObj2loc >= 0){
				dependMatrix[compObj1loc][compObj2loc] = 1;
			}
			else{
				logger.error("Exluding systems " + compObj1 + " and " + compObj2);
			}
		}

		return dependMatrix;
	}

	/**
	 * @param systemMatrix
	 */
	private static List<List<Integer>> createGroups(Integer[][] dependMatrix)
	{

		Map<String, ArrayList<Integer>> groups = new HashMap<>();
		for( int idx = 0; idx < dependMatrix.length ; idx++  ) {
			//find next group
			List<Integer> procRows = new ArrayList<>();
			logger.debug("Adding row "+idx+" to processed list");
			ArrayList<Integer> systemRow = new ArrayList<>();
			assembleGroup(idx, procRows, dependMatrix, systemRow);
			Collections.sort(systemRow);
			groups.put(Arrays.toString(systemRow.toArray()), systemRow);
		}

		return new ArrayList(groups.values());
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
				logger.error("this is strange... adding "+ row +" again to " + rowArray);
			}
			logger.debug("Marked row "+row);
			rowArray.add(row);

			//mark row as processed. removes possible loops
			//find all dependencies of row
			for(int j=0; j < dependMatrix[row].length; j++){
				if(dependMatrix[row][j] != null && dependMatrix[row][j] == 1 && !procRows.contains(j)){
					logger.debug("System "+j+" depends on "+row);
					assembleGroup(j, procRows, dependMatrix, rowArray);
				}
			}
		}

		logger.debug(rowArray);
		return;
	}

	@Override
	public void setQuery(String query) {

		String[] items = query.split("\\+{3}");

		logger.info("Setting comparison object query as " + items[0]);
		this.compObjQuery = items[0];

		logger.info("Setting dependency objects query as " + items[1]);
		this.depObjQuery = items[1];

		logger.info("Setting dependency query as " + items[2]);
		this.depQuery = items[2];

		addtlQueries = new ArrayList<>();
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

//		PropertyConfigurator.configure("log4j.prop");

		Integer[][] testSystems = new Integer [5][5];

		testSystems[0][0] = null;
		testSystems[0][1] = 1;
		testSystems[0][2] = null;
		testSystems[0][3] = null;
		testSystems[0][4] = null;
		testSystems[1][0] = null;
		testSystems[1][1] = null;
		testSystems[1][2] = 1;
		testSystems[1][3] = 1;
		testSystems[1][4] = 1;
		testSystems[2][0] = null;
		testSystems[2][1] = null;
		testSystems[2][2] = null;
		testSystems[2][3] = 1;
		testSystems[2][4] = 1;
		testSystems[3][0] = null;
		testSystems[3][1] = null;
		testSystems[3][2] = 1;
		testSystems[3][3] = null;
		testSystems[3][4] = 1;
		testSystems[4][0] = null;
		testSystems[4][1] = null;
		testSystems[4][2] = 1;
		testSystems[4][3] = 1;
		testSystems[4][4] = null;


		List<List<Integer>> groups = createGroups(testSystems);
		Map<Integer, List<List<Integer>>> decomGroups = createDecommissioningGroups(groups, new HashMap()); 

		ArrayList <Object []> list = new ArrayList<>();
		int groupCounter = 0;

		//key is counter for one level above group
		for(Integer key: decomGroups.keySet()){
			for(List<Integer> depGroup: decomGroups.get(key)){
				for(Integer compObj: depGroup){
					String keyToGroupCounter = new String(key.toString()+"."+groupCounter);
					double location = Double.parseDouble(keyToGroupCounter);
					Object[] depObj = new Object[]{compObj, location};
					list.add(depObj);
					logger.info("Added ["+depObj[0]+", "+depObj[1]+"]");
				}
				groupCounter++;
			}
			groupCounter = 0;
		}

	}

	@Override
	public Hashtable getDataMakerOutput(String... selectors){
		Hashtable returnHash = OUSDPlaysheetHelper.getData(this.title, this.questionNum, this.dataFrame, PlaySheetRDFMapBasedEnum.getSheetName("Grid"));
		return returnHash;
	}

}