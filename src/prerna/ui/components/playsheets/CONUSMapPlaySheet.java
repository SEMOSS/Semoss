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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import prerna.engine.api.ISelectStatement;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;

/**
 * The Play Sheet for Continental United States (CONUS) geo-location data.  
 * Visualizes Latitude/Longitude coordinates on a map of the CONUS.
 */
public class CONUSMapPlaySheet extends BrowserPlaySheet {

	private static final Logger logger = LogManager.getLogger(CONUSMapPlaySheet.class.getName());
	Hashtable allHash;
	HashSet data;
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
	public Hashtable processQueryData()
	{
		data = new HashSet();
		String[] var = wrapper.getVariables(); 	
		
		for (int i=0; i<list.size(); i++)
		{	
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
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

		allHash = new Hashtable();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", var[1]);
		allHash.put("lon", var[2]);
		if (var.length > 3 && !var[3].equals(null))
			allHash.put("size", var[3]);
		else
			allHash.put("size", "size");
		allHash.put("locationName", var[0]);
//		allHash.put("xAxisTitle", var[0]);
//		allHash.put("yAxisTitle", var[1]);
//		allHash.put("value", var[2]);
		
		
		return allHash;
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

		browser.executeJavaScript("start('" + gson.toJson(table) + "');");
		output.clear();
		allHash.clear();
		data.clear();
	}
	
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		Object var = sjss.getRawVar(varName);
			if( var != null && var instanceof Literal) {
				var = sjss.getVar(varName);
			} 
		return var;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		alignHash.put("label", names[0]);
		alignHash.put("lat", names[1]);
		alignHash.put("lon", names[2]);
		if (names.length > 3 && !names[3].equals(null))
			alignHash.put("size", names[3]);
		return alignHash;
	}
}
