/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import com.google.gson.Gson;

/**
 * The Play Sheet for Outside the Continental United States (OCONUS) geo-location data.  
 * Visualizes Latitude/Longitude coordinates on a map of OCONUS.
 */
public class OCONUSMapPlaySheet extends BrowserPlaySheet {

	Hashtable allHash;
	HashSet data;
	/**
	 * Constructor for OCONUSMapPlaySheet.
	 */
	public OCONUSMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/worldmap.html";
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
		
		//Possibly filter out all US Facilities from the query?
		
		for (int i=0; i<list.size(); i++)
		{	
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
			String colName;
			Double value;
			for (int j = 0; j < var.length; j++) 
			{	
				colName = var[j];
				elementHash.put("size", 1000000);
						if (listElement[j] instanceof String)
						{	
							String text = (String) listElement[j];
							elementHash.put(colName, text);
						}
						else 
						{	
							value = (Double) listElement[j];							
							elementHash.put(colName, value);
						}
								
			}	
				data.add(elementHash);			
		}

		allHash = new Hashtable();
		allHash.put("dataSeries", data);
		
		allHash.put("lat", "lat" );
		allHash.put("lon", "lon");
		allHash.put("size", "size");
		allHash.put("locationName", var[0]);
		/*allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);*/
		
		
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

		browser.executeScript("start('" + gson.toJson(table) + "');");
		output.clear();
		allHash.clear();
		data.clear();
	}
}
