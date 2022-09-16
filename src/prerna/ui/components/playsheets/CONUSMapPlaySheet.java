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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * The Play Sheet for Continental United States (CONUS) geo-location data.  
 * Visualizes Latitude/Longitude coordinates on a map of the CONUS.
 */
public class CONUSMapPlaySheet extends BrowserPlaySheet {

	private static final Logger logger = LogManager.getLogger(CONUSMapPlaySheet.class.getName());
	Hashtable<String, Object> allHash;
	HashSet<LinkedHashMap<String, Object>> data;
	private static final String labelString = "label";
	private static final String latString = "lat";
	private static final String lonString = "lon";
	private static final String sizeString = "size";
	
	
	/**
	 * Constructor for CONUSMapPlaySheet.
	 */
	public CONUSMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/conusmap.html";
	}
	
	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - A Hashtable of the queried Continental United States data to be converted into json format.  
	 * The data must be in the format lat, lon, size, and must include any relevant properties for the coordinate.
	 */
	public void processQueryData()
	{
		data = new HashSet<LinkedHashMap<String, Object>>();
		String[] var = dataFrame.getColumnHeaders();

		Map<String, String> align = getDataTableAlign();
		int labelIdx = -1;
		int latIdx = -1;
		int lonIdx = -1;
		int sizeIdx = -1;
		for(int i = 0; i < var.length; i++){
			String name = var[i];
			if(align.containsValue(name)){
				String key = Utility.getKeyFromValue(align, name);
				if(key.contains(labelString)){
					labelIdx = i;
				}
				else if(key.contains(latString)){
					latIdx = i;
				}
				else if(key.contains(lonString)){
					lonIdx = i;
				}
				else if(key.contains(sizeString)){
					sizeIdx = i;
				}
			}
		}
		
		Iterator<IHeadersDataRow> it = dataFrame.iterator();
		
		while(it.hasNext())
		{	
			LinkedHashMap<String, Object> elementHash = new LinkedHashMap<String, Object>();
			Object[] listElement = it.next().getValues();
			String colName;
			Double value;
			for (int j = 0; j < var.length; j++) 
			{	
				colName = var[j];
				elementHash.put("size", "size");
				if (listElement[j] instanceof String)
				{	
					String text = (String) listElement[j];
					elementHash.put(colName, text);
				}
				else if (listElement[j] instanceof Double) {
					value = (Double) listElement[j];							
					elementHash.put(colName, value);
				}
				else 
				{	
					elementHash.put(colName, listElement[j]);
				}
								
			}
			data.add(elementHash);			
		}

		allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", var[latIdx]);
		allHash.put("lon", var[lonIdx]);
		if (sizeIdx != -1)
			allHash.put("size", var[sizeIdx]);
		else
			allHash.put("size", "size");
		allHash.put("locationName", var[labelIdx]);
//		allHash.put("xAxisTitle", var[0]);
//		allHash.put("yAxisTitle", var[1]);
//		allHash.put("value", var[2]);
		
		
		this.dataHash = allHash;
	}
	
	@Override
	/**
	 * Method callIt.  Converts a given Hashtable to a Json and passes it to the browser.
	 * @param table Hashtable - the correctly formatted data from the SPARQL query results.
	 */
	public void callIt(Hashtable table)
	{
		output = table;
		Gson gson = new Gson();
		//logger.info("Converted " + gson.toJson(table));
		logger.info("Converted gson");

//		browser.executeJavaScript("start('" + gson.toJson(table) + "');");
		output.clear();
		allHash.clear();
		data.clear();
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
		
		alignHash.put(labelString, names[0]);
		alignHash.put(latString, names[1]);
		alignHash.put(lonString, names[2]);
		if (names.length > 3 && names[3] != null) {
			alignHash.put(sizeString, names[3]);
		}
		return alignHash;
	}
}
