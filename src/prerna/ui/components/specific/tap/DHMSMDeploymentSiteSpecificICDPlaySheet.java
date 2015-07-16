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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.ListUtilityMethods;

public class DHMSMDeploymentSiteSpecificICDPlaySheet extends GridPlaySheet {
	
	HashMap<String, ArrayList<String>> siteData;
	Set<String> centrallyDeployedSystems;

	List<Object[]> list;
	String[] names;
	
	@Override
	public List<Object[]> getTabularData() {
		return this.list;
	}
	
	@Override
	public String[] getColumnHeaders() {
		return this.names;
	}
	
	@Override
	public void createData() {
		ArrayList<Object[]> retList = new ArrayList<Object[]>();
		
		DualEngineGridPlaySheet degp = new DualEngineGridPlaySheet();
		degp.setQuery(query);
		degp.createData();
		List<Object[]> combinedResults = degp.getTabularData();
		names = degp.getNames();
		
		getSysData();
		Set<String> icdList = new HashSet<String>();
		Iterator<Object[]> resultIterator = combinedResults.iterator();
		while(resultIterator.hasNext()) {
			Object[] values = resultIterator.next();
			String site = values[0].toString();
			ArrayList<String> sysList = siteData.get(site);
			sysList.add("DHMSM");
			String sys1 = values[1].toString();
			String sys2 = values[2].toString();
			String icd = values[3].toString();
			if( (sysList.contains(sys1) && sysList.contains(sys2)) || 
					(centrallyDeployedSystems.contains(sys1) || centrallyDeployedSystems.contains(sys2)) ) {
				if(!icdList.contains(icd)) {
					// in order to not repeat ICDs
					icdList.add(icd);
					retList.add(values);
				}
			}
		}
		
		list = ListUtilityMethods.removeColumnFromList(retList, 1);
		list = ListUtilityMethods.removeColumnFromList(list, 1);
		names = ArrayUtilityMethods.removeNameFromList(names, 1);
		names = ArrayUtilityMethods.removeNameFromList(names, 1);
		
		if(list == null) {
			list = new ArrayList<Object[]>();
		}
		if(names == null) {
			names = new String[]{};
		}
	}
	
	public void getSysData() {
		if(siteData == null) {
			siteData = DHMSMDeploymentHelper.getSysAtSitesInDeploymentPlan(engine);
		}
		if(centrallyDeployedSystems == null) {
			IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			centrallyDeployedSystems = DHMSMDeploymentHelper.getCentrallyDeployedSystems(hrCore);
		}
	}
}
