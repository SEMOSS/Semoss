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
import java.util.Hashtable;
import java.util.Iterator;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DrillDownPlaySheet extends BrowserPlaySheet {

	public DrillDownPlaySheet() {
		// TODO Auto-generated constructor stub
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/drilldown.html";
	}

	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.

	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public void processQueryData()
	{
		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		String[] var = dataFrame.getColumnHeaders();

		for(int i = 1;i<var.length;i++) {
			Hashtable<String,ArrayList<String>> elementHash = new Hashtable<String,ArrayList<String>>();
			
			Iterator<Object[]> it = dataFrame.iterator(false, null);
			while(it.hasNext()) {
				ArrayList<String> listElementArray = new ArrayList<String>();
				Object[] listElement = it.next();
				if (listElement[i]== null || ((String)listElement[i]).isEmpty()) {
					continue;
				}
				String systemService = (String) listElement[0];
				if (elementHash.get(systemService) != null) {
					listElementArray = elementHash.get(systemService);
				}
				listElementArray.add((String)listElement[i]);
				elementHash.put(systemService, listElementArray);
			}
			String key = var[i];
			dataHash.put(key, elementHash);
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", dataHash);		
		allHash.put("xAxisTitle", "Node Type");
		allHash.put("yAxisTitle", "System Service");


		this.dataHash = allHash;
	}


}
