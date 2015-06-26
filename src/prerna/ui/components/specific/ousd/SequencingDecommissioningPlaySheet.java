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
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.DIHelper;

public class SequencingDecommissioningPlaySheet extends BasicProcessingPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());
	Map dataHash = new Hashtable();

	List<String> dataObjectList;
	List<String> systemList;
	
	String dataListQuery;
	String sysListQuery;
	
	public SequencingDecommissioningPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		// populate data object list with list of all data objects (query should be passed in)
		this.dataObjectList = new ArrayList<String>();
//		this.dataListQuery;
		//wrapper manager, etc. to fill data obj list
		
		// populate system list with list of all systems (query should also be passed in)
		this.systemList = new ArrayList<String>();
//		this.sysListQuery;
		//wrapper manager, etc. to fill sys list
		
		Map<String, Integer[][]> dependMatrices = createDependencyMatrices();
		createDecommissioningGroups(dependMatrices);
	}
	
	private Map<String, Integer[][]> createDependencyMatrices(){
		// iterate through list and for each:
		// create the data network of systems
		// iterate down through the network, capturing :
		//   1. the level that the system is on wrt that data object
		//   2. the system - system dependencies
		// (is it possible that we have islands? can a system exist outside of the main network?)
		
		Integer[][] sysDependMatrix = new Integer[systemList.size()][systemList.size()];
		Integer[][] dataDependMatrix = new Integer[systemList.size()][dataObjectList.size()];
		
		
		//iterate through list and for each:
		for (String dataObj : this.dataObjectList){
			// construct data network
			//iterate down
			//populate matrix
			// LOOK TO LEVERAGE DistanceDownstreamProcessor
		}
		
		Map<String, Integer[][]> returnObj = new HashMap<String, Integer[][]>();
		returnObj.put("sysDepend", sysDependMatrix);
		returnObj.put("dataDepend", dataDependMatrix);
		return returnObj;
	}
	
	private void createDecommissioningGroups(Map<String, Integer[][]> dependMatrices){
		// check for zeroes
		// identify best rows based on maximums in dataDepend
		// iterate through best rows
		// run recursive method
		// not sure what exactly we want the output to be....
	}
	
	@Override
	public void setQuery(String query) {

		String[] items = query.split("+{3}");

		LOGGER.info("Setting system query as " + items[0]);
		this.sysListQuery = items[0];

		LOGGER.info("Setting system query as " + items[1]);
		this.dataListQuery = items[1];
	}
	
	@Override
	public Object getData() {
		Hashtable returnHash = (Hashtable) super.getData();
		if (dataHash != null)
			returnHash.put("specificData", dataHash);
		return returnHash;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// once we determine display, we will use this to align data to display
		return null;
	}
}
