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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * The Play Sheet for creating a Parallel Sets diagram.
 */
public class ParallelSetsPlaySheet extends BrowserPlaySheet {

	private static final String dimString = "dim ";
	
	/**
	 * Constructor for ParallelSetsPlaySheet.
	 */
	public ParallelSetsPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/parsets.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Processed text and numerical data accordingly for the parallel sets visualization.*/
	public void processQueryData()
	{
		ArrayList<Map<String, Object>> dataArrayList = new ArrayList<Map<String, Object>>();
		String[] var = dataFrame.getColumnHeaders();

		Map<String, String> align = getDataTableAlign();
		Integer[] seriesIndices = new Integer[align.size()];
		for(int i = 0; i < var.length; i++){
			String name = var[i];
			if(align.containsValue(name)){
				String key = Utility.getKeyFromValue(align, name);
				if(key.contains(dimString)){
					int seriesIdx = Integer.parseInt(key.replace(dimString, ""));
					seriesIndices[seriesIdx] = i;
				}
			}
		}
		
		Iterator<Object[]> it = dataFrame.iterator();
		while(it.hasNext())
		{	
			LinkedHashMap<String, Object> elementHash = new LinkedHashMap<String, Object>();
			Object[] listElement = it.next();
			String colName;
			Double value;
			for (int j = 0; j < seriesIndices.length; j++) 
			{
				int idx = seriesIndices[j];
				colName = var[idx];
				if (listElement[idx] instanceof String)
				{
					String text = (String) listElement[idx];
					text = text.replaceAll("^\"|\"$", "");
					if (text.length() >= 30) {
					text = text.substring(0, Math.min(text.length(), 30));  //temporary
					text = text + "...";
					}
					elementHash.put(colName, text);
				}
				else if(listElement[idx] instanceof java.lang.Float){
					Float valueFloat = (float) listElement[idx];
					value = valueFloat.doubleValue();
					elementHash.put(colName, value);
					
				} else 
				{
					value = (Double) listElement[idx];	
					elementHash.put(colName, value);
				}
			}
				dataArrayList.add(elementHash);			
		}
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", dataArrayList);
		allHash.put("headers", var);
		
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

		for(int namesIdx = 0; namesIdx<names.length; namesIdx++){
			alignHash.put(dimString + namesIdx, names[namesIdx]);
		}
		return alignHash;
	}
}
