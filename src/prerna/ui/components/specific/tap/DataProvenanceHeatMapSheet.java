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
import java.util.Hashtable;
import java.util.Iterator;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DataProvenanceHeatMapSheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 */
	public DataProvenanceHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/dataprovenance.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public void processQueryData()
	{
		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		String[] var = wrapper.getVariables();
		String xName = var[0];
		String yName = var[1];
		
		Iterator<Object[]> it = dataFrame.iterator(false, null);
		while(it.hasNext()) {	
			Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
			Object[] listElement = it.next();
			String methodName = (String) listElement[0];
			String groupName = (String) listElement[1];
			methodName = methodName.replaceAll("\"", "");
			groupName = groupName.replaceAll("\"", "");
			String key = methodName +"-"+groupName;
			String crm = (String) listElement[2];
			crm = crm.replaceAll("\"", "");
			double count = 1;
			elementHash.put(xName, methodName);
			elementHash.put(yName, groupName);
			if (crm.equals("C")) {
				count = 1;
			}
			else if (crm.equals("R") && !dataHash.containsKey(key)) {
				count = 0;
			}			
			elementHash.put(var[2], count);
			dataHash.put(key, elementHash);
			
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  var[0] + " vs " + var[1]);
		allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);
		
		this.dataHash = allHash;
	}
	

}
