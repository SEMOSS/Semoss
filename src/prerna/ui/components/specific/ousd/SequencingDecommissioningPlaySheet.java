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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.Utility;

public class SequencingDecommissioningPlaySheet extends BasicProcessingPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SequencingDecommissioningPlaySheet.class.getName());
	Map dataHash = new Hashtable();

	String INTERFACE_QUERY = "SELECT DISTINCT ?System2 ?System3 WHERE { {?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payloads>;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Interface_Control_Document>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provides>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consumes>;} {?System2 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;} } BINDINGS ?Data1 {@Data-Data@}";
	List<String> systemList;
	
	String dataListQuery;
	String dataBindingsString;
	String sysListQuery;
	
	public SequencingDecommissioningPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		// populate data object list with list of all data objects (query should be passed in)
		ISelectWrapper dataWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.dataListQuery);
		//wrapper manager, etc. to fill data obj list
		String[] dataNames = dataWrap.getVariables();
		dataBindingsString = "";
		while(dataWrap.hasNext()){
			String thisData = dataWrap.next().getRawVar(dataNames[0])+"";
			dataBindingsString = dataBindingsString + "(<"+thisData + ">)";
		}
		
		// populate system list with list of all systems (query should also be passed in)
		this.systemList = new ArrayList<String>();
		ISelectWrapper sysWrap = WrapperManager.getInstance().getSWrapper(this.engine, this.sysListQuery);
		//wrapper manager, etc. to fill sys list
		String[] sysNames = sysWrap.getVariables();
		while(sysWrap.hasNext()){
			this.systemList.add(sysWrap.next().getRawVar(sysNames[0])+"");
		}
		
		Integer[][] dependMatrix = createDependencyMatrices();
		createDecommissioningGroups(dependMatrix);
	}
	
	private Integer[][] createDependencyMatrices(){
		// iterate through list and for each:
		// create the data network of systems
		// iterate down through the network, capturing :
		//   1. the level that the system is on wrt that data object
		//   2. the system - system dependencies
		// (is it possible that we have islands? can a system exist outside of the main network?)
		
		Integer[][] sysDependMatrix = new Integer[systemList.size()][systemList.size()];
		String unfilledQuery = this.INTERFACE_QUERY;
		Hashtable<String, String> paramHash = new Hashtable<String, String>();
		paramHash.put("Data-Data", dataBindingsString);
		String query = Utility.fillParam(unfilledQuery, paramHash);
		
		ISelectWrapper icdWrap = WrapperManager.getInstance().getSWrapper(this.engine, query);
		//wrapper manager, etc. to fill sys list
		String[] icdNames = icdWrap.getVariables();
		while(icdWrap.hasNext()){
			ISelectStatement ss = icdWrap.next();
			String sys1 = ss.getRawVar(icdNames[0])+"";
			String sys2 = ss.getRawVar(icdNames[1])+"";
			LOGGER.info(" Found relation:::: " + sys1 + " depends on " + sys2);
			
			int sys1loc = this.systemList.indexOf(sys1);
			int sys2loc = this.systemList.indexOf(sys2);
			LOGGER.info(" Found location:::: " + sys1loc + " and " + sys2loc);
			
			if(sys1loc >= 0 && sys2loc >= 0){
				sysDependMatrix[sys1loc][sys2loc] = 1;
			}
			else{
				LOGGER.error("Exluding systems " + sys1 + " and " + sys2);
			}
		}
		
		return sysDependMatrix;
	}
	
	private void createDecommissioningGroups(Integer[][] dependMatrices){
		// check for zeroes
		// identify best rows based on maximums in dataDepend
		// iterate through best rows
		// run recursive method
		// not sure what exactly we want the output to be....
	}
	
	@Override
	public void setQuery(String query) {

		String[] items = query.split("\\+{3}");

		LOGGER.info("Setting system query as " + items[0]);
		this.sysListQuery = items[0];

		LOGGER.info("Setting data query as " + items[1]);
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
