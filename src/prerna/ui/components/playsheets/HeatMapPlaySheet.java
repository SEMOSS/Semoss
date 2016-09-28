/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * The Play Sheet for creating a heat map diagram.  
 */
public class HeatMapPlaySheet extends BrowserPlaySheet {
	
	private static String xString = "x";
	private static String yString = "y";
	private static String heatString = "heat";
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 */
	public HeatMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/heatmap.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public void processQueryData()
	{
		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		String[] var = dataFrame.getColumnHeaders();
		Map<String, String> align = getDataTableAlign();
		int heatIdx = -1;
		int xIdx = -1;
		int yIdx = -1;
		for(int i = 0; i < var.length; i++){
			String name = var[i];
			if(align.containsValue(name)){
				String key = Utility.getKeyFromValue(align, name);
				if(key.contains(xString)){
					xIdx = i;
				}
				else if(key.contains(yString)){
					yIdx = i;
				}
				else if(key.contains(heatString)){
					heatIdx = i;
				}
			}
		}
		String xName = var[xIdx];
		String yName = var[yIdx];
		
		Iterator<Object[]> it = dataFrame.iterator();
		while(it.hasNext())
		{
			Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
			Object[] listElement = it.next();		
			String methodName = listElement[xIdx].toString();
			String groupName = listElement[yIdx].toString();
			String key = methodName +"-"+groupName;
			double count = (Double) listElement[heatIdx];
			elementHash.put(xName, methodName);
			elementHash.put(yName, groupName);
			elementHash.put(var[heatIdx], count);
			dataHash.put(key, elementHash);
			
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  var[xIdx] + " vs " + var[yIdx]);
		allHash.put("xAxisTitle", var[xIdx]);
		allHash.put("yAxisTitle", var[yIdx]);
		allHash.put("value", var[heatIdx]);
		
		this.dataHash = allHash;
	}

	@Override
	public Map<String, String> getDataTableAlign() {
		if(this.tableDataAlign == null){
			this.tableDataAlign = getAlignHash();
		}
		return this.tableDataAlign;
	}
	
	public Hashtable<String, String> getAlignHash() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		String[] names = dataFrame.getColumnHeaders();
		
		alignHash.put(xString, names[0]);
		alignHash.put(yString, names[1]);
		alignHash.put(heatString, names[2]);
		return alignHash;
	}
}
