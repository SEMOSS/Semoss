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
import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class SysDecommissionOptimizationPlaySheet extends GridPlaySheet{

	public int resource;
	public double time;
	
	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getTabularData() {
		return this.list;
	}
	
	@Override
	public String[] getNames() {
		return this.names;
	}
	
	@Override
	public void createData() {
		
		String db = "HR_Core";
		String sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} FILTER(?Probability in('High','Question')) }";
		String dataQuery = "SELECT DISTINCT ?Data WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}}";

		ArrayList<String> sysList = runListQuery(db, sysQuery);
		ArrayList<String> dataList = runListQuery(db, dataQuery);

		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		
		names = new String[9];
		names[0] = "System";
		names[1] = "Probability";
		names[2] = "Minimum Time to Transform (Years)";
		names[3] = "Time to Transform at All Sites (Years)";
		names[4] = "Work Volume for One Site (Years)";
		names[5] = "Number of Sites Deployed At";
		names[6] = "Resource Allocation";
		names[7] = "Number of Systems Transformed Simultaneously";
		names[8] = "Total Cost for System";
//		names[5] = "Min time for system";
		
		optFunctions.setSysList(sysList);
		optFunctions.setDataList(dataList);;
		if(query.equals("Constrain Resource"))
		{
			optFunctions.resourcesConstraint = resource;
			optFunctions.optimizeTime();

		}
		else
		{
			optFunctions.timeConstraint = time;
			optFunctions.optimizeResource();
		}

		list = optFunctions.outputList;
	}

	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public ArrayList<String> runListQuery(String engineName, String query) {
		ArrayList<String> list = new ArrayList<String>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

	
			/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			
			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
				}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
		return list;
	}
	
	
	public void runPlaySheet(String typeConstraint,int resource, double time) {
		query = typeConstraint;
		this.resource = resource;
		this.time = time;
		createData();
		runAnalytics();
		createView();
	}

}
