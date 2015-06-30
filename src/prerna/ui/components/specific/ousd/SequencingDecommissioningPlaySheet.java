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
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;

public class SequencingDecommissioningPlaySheet extends GridPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());
//	Map dataHash = new Hashtable();

//	String INTERFACE_QUERY = "SELECT DISTINCT ?System2 ?System3 WHERE { {?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payloads>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Interface_Control_Document>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provides>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consumes>;} {?System2 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;} } BINDINGS ?Data1 {@Data-Data@}";
	List<String> compObjList;
	
	String depQuery;
	String compObjQuery;
	String depObjBindingsString;
	String depObjQuery;
	
	public SequencingDecommissioningPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		// populate data object list with list of all data objects (query should be passed in)
		ISelectWrapper depObjWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.depObjQuery);
		//wrapper manager, etc. to fill data obj list
		String[] depObjNames = depObjWrap.getVariables();
		depObjBindingsString = "";
		while(depObjWrap.hasNext()){
			String thisDepObj = depObjWrap.next().getRawVar(depObjNames[0])+"";
			depObjBindingsString = depObjBindingsString + "(<"+thisDepObj + ">)";
		}
		
		// populate system list with list of all systems (query should also be passed in)
		this.compObjList = new ArrayList<String>();
		ISelectWrapper compObjWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.compObjQuery);
		//wrapper manager, etc. to fill sys list
		String[] compObjNames = compObjWrap.getVariables();
		while(compObjWrap.hasNext()){
			this.compObjList.add(compObjWrap.next().getRawVar(compObjNames[0])+"");
		}
		
		Integer[][] dependMatrix = createDependencyMatrices();
        HashMap<Integer, List<ArrayList<Integer>>> groups = createDecommissioningGroups(dependMatrix);
        int groupCounter = 0;
        
        list = new ArrayList<Object[]>();
        //key is counter for one level above group
        for(Integer key: groups.keySet()){
              for(ArrayList<Integer> depGroup: groups.get(key)){
                    for(Integer compObj: depGroup){
                          String keyToGroupCounter = new String(key.toString()+"."+groupCounter);
                          double location = Double.parseDouble(keyToGroupCounter);
                          Object[] depObj = new Object[]{compObjList.get(compObj), location};
                          list.add(depObj);
                          System.out.println("added object "+depObj);
                    }
                    groupCounter++;
              }
              groupCounter = 0;
        }
        
		// set the names
		this.names = new String[]{"Comparison Object", "Group Value"};
	}
	
	private Integer[][] createDependencyMatrices(){
		// iterate through list and for each:
		// create the data network of systems
		// iterate down through the network, capturing :
		//   1. the level that the system is on wrt that data object
		//   2. the system - system dependencies
		// (is it possible that we have islands? can a system exist outside of the main network?)
		
		Integer[][] dependMatrix = new Integer[compObjList.size()][compObjList.size()];
		String query = this.depQuery.replace("~~DepObj-DepObj~~", depObjBindingsString);
		
		ISelectWrapper depWrap = WrapperManager.getInstance().getSWrapper(this.engine, query);
		//wrapper manager, etc. to fill sys list
		String[] depNames = depWrap.getVariables();
		while(depWrap.hasNext()){
			ISelectStatement ss = depWrap.next();
			String compObj1 = ss.getRawVar(depNames[0])+"";
			String compObj2 = ss.getRawVar(depNames[1])+"";
			LOGGER.info(" Found relation:::: " + compObj1 + " depends on " + compObj2);
			
			int compObj1loc = this.compObjList.indexOf(compObj1);
			int compObj2loc = this.compObjList.indexOf(compObj2);
			LOGGER.info(" Found location:::: " + compObj1loc + " and " + compObj2loc);
			
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
			System.out.println("mapping row "+i+" to total "+rowTotal);
			rowSums.put(i, rowTotal);
		}
		
		//loop until every system has been processed
		while(procRows.size() != dependMatrix.length){
			//find next group
			List<Integer> currentGroup = findNextGroup(rowSums, procRows, dependMatrix);
			
			//BILLS BEAUTIFUL METHOD
			//recursive method through group and assign group number
			List<ArrayList<Integer>> group = new ArrayList<ArrayList<Integer>>();
			for(Integer row: currentGroup){
				System.out.println("adding row "+row+" to processed list");
				ArrayList<Integer> systemRow = new ArrayList<Integer>();
				assembleGroup(row, procRows, dependMatrix, systemRow);
				if(!systemRow.isEmpty()){
					group.add(systemRow);
				}
			}
			groups.put(groupNumber, group);
			groupNumber++;
			/////////////// END BILL
			
			/* 
			////////////SPENCER'S CRAPPY METHOD
			for(Integer row: currentGroup){
                  System.out.println("adding row "+row+" to processed list");
            }
            //recursive method through group and assign group number
            ArrayList<Integer> systemRows = new ArrayList<Integer>();
            List<ArrayList<Integer>> sysGroup = new ArrayList<ArrayList<Integer>>();
            groups.put(groupNumber, assembleGroup(currentGroup, procRows, dependMatrix, systemRows, sysGroup));
            groupNumber++;
            //////////////END SPENCER
            */
            
			//update map
			rowSums = updateRowTotals(rowSums, procRows, dependMatrix);
			System.out.println("-----------------------");
		}
		
		for(Integer key: groups.keySet()){
			System.out.println("group is "+key);
			System.out.println(groups.get(key));
		}

		return groups;
		// check for zeroes

		// run recursive method

		// not sure what exactly we want the output to be....
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
					System.out.println("new low total ("+currentLow+") for row "+key);
					lowGroup.clear();
					lowGroup.add(key);
				}else if(rowMap.get(key) == currentLow){
					System.out.println("matching low found for row "+key);
					lowGroup.add(key);
				}
			}else{
				continue;
			}
		}
		System.out.println(lowGroup);
		return lowGroup;
	}
	
	private static HashMap<Integer, Integer> updateRowTotals(HashMap<Integer, Integer> rowTotals, List<Integer> procRows, Integer[][] systemMatrix){
		for(Integer key: rowTotals.keySet()){
			int newTotal = 0;
			System.out.println("updating row "+key);
			for(int j=0; j < systemMatrix[key].length; j++){
				if(systemMatrix[key][j] != null && !procRows.contains(j)){
					System.out.println("added system "+j+" to total for row "+key);
					newTotal = newTotal + systemMatrix[key][j];
					System.out.println("new total for row "+key+" is "+newTotal);
					rowTotals.put(key, newTotal);
				}else if(procRows.contains(j)){
					System.out.println("row "+j+" was already processed. skipping row");
				}
			}
		}
		return rowTotals;
	}
	
	private static void assembleGroup(Integer row, List<Integer> procRows, Integer[][] dependMatrix, ArrayList<Integer> rowArray){

		//add row first time
		if(procRows.contains(row)){
			return;
		}else{
			procRows.add(row);
			if(rowArray.contains(row)){
				System.err.println("this is strange... adding "+ row +" again to " + rowArray);
			}
			System.out.println("marked row "+row);
			rowArray.add(row);
			
			//mark row as processed. removes possible loops
			//find all dependencies of row
			System.out.println("finding dependencies of row "+row);
			for(int j=0; j < dependMatrix[row].length; j++){
				if(dependMatrix[row][j] != null && dependMatrix[row][j] == 1 && !procRows.contains(j)){
					System.out.println("system "+j+" depends on "+row);
					assembleGroup(j, procRows, dependMatrix, rowArray);
				}
			}
		}
			
		System.out.println(rowArray);
		return;
	}
	
	/**
	 * @param levelGroup
	 * @param procRows
	 * @param systemMatrix
	 * @param systemRows
	 * @param sysGroup
	 * @return
	 */
	private static List<ArrayList<Integer>> assembleGroup(List<Integer> levelGroup, List<Integer> procRows, Integer[][] systemMatrix, ArrayList<Integer> systemRows, List<ArrayList<Integer>> sysGroup){

		for(Integer row: levelGroup){
			ArrayList<Integer> dependencies = new ArrayList<Integer>();
			//add row first time
			if(procRows.contains(row)){
				continue;
			}else{
				//mark row as processed. removes possible loops
				procRows.add(row);
				System.out.println("marked row "+row);
				systemRows.add(row);
				System.out.println(dependencies);

				//find all dependencies of row
				System.out.println("finding dependencies of row "+row);
				for(int j=0; j < systemMatrix[row].length; j++){
					if(systemMatrix[row][j] != null && systemMatrix[row][j] == 1 && !procRows.contains(j)){
						System.out.println("system "+j+" depends on "+row);
						dependencies.add(j);
						assembleGroup(dependencies, procRows, systemMatrix, systemRows, sysGroup);
					}
				}
			}
			if(!dependencies.isEmpty()){
				for(Integer dependent: dependencies){
					if(!procRows.contains(dependent)){
						if(!systemRows.contains(dependent)){
							systemRows.add(dependent);
						}						
					}
					systemRows = new ArrayList<Integer>();
				}
			}else if(dependencies.isEmpty()){
				System.out.println("row "+row+" had no dependencies");
				sysGroup.add(systemRows);
				systemRows = new ArrayList<Integer>();
				continue;
			}
			dependencies.clear();
		}
		System.out.println(sysGroup);
		return sysGroup;
	}
	
	public static void main(String args[]){
		Integer[][] testData = new Integer[5][2];
		Integer[][] testSystems = new Integer [5][5];
		
		testSystems[0][0] = null;
		testSystems[0][1] = null;
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
	
		testData[0][0] = 2;
		testData[0][1] = 1;
		testData[1][0] = 2;
		testData[1][1] = 2;
		testData[2][0] = 3;
		testData[2][1] = 0;
		testData[3][0] = 4;
		testData[3][1] = 4;
		testData[4][0] = 1;
		testData[4][1] = 3;
		
		createDecommissioningGroups(testSystems);
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
}
