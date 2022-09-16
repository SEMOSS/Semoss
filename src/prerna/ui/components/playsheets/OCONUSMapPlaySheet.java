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
 * The Play Sheet for Outside the Continental United States (OCONUS) geo-location data. Visualizes Latitude/Longitude
 * coordinates on a map of OCONUS.
 */
public class OCONUSMapPlaySheet extends BrowserPlaySheet {
	
	private static final Logger logger = LogManager.getLogger(OCONUSMapPlaySheet.class.getName());
	protected HashSet<LinkedHashMap<String, Object>> data;
	protected Hashtable<String, Object> allHash;
	private static final String labelString = "label";
	private static final String seriesString = "series"; //optional
	private static final String latString = "lat";
	private static final String lonString = "lon";
	private static final String sizeString = "size"; //optional
	private static final String timeString = "time"; //optional
	
	/**
	 * Constructor for OCONUSMapPlaySheet.
	 */
	public OCONUSMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(1600, 1150));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/worldmap.html";
	}
	
	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an appropriate format for the specific play
	 * sheet.
	 * 
	 * @return Hashtable - A Hashtable of the queried Continental United States data to be converted into json format. The
	 *         data must be in the format lat, lon, size, and must include any relevant properties for the coordinate.
	 */
	public void processQueryData() {
		Map<String, String> align = getDataTableAlign();
		int labelIdx = -1;
		int latIdx = -1;
		int lonIdx = -1;
		int sizeIdx = -1; //optional
		int seriesIdx = -1; //optional
		int timeIdx = -1; //optional
		data = new HashSet<LinkedHashMap<String, Object>>();
		String[] names = dataFrame.getColumnHeaders();
		
		for(int i = 0; i < names.length; i++){
			String name = names[i];
			if(align.containsValue(name)){
				String key = Utility.getKeyFromValue(align, name);
				if(key.contains(labelString)){
					labelIdx = i;
				}
				else if(key.contains(seriesString)){
					seriesIdx = i;
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
				else if(key.contains(timeString)) {
					timeIdx = i;
				}
			}
		}
		
		String name = names[labelIdx];
		// Possibly filter out all US Facilities from the query?
		Iterator<IHeadersDataRow> it = dataFrame.iterator();
		ArrayList<Hashtable<String, Object>> allData = new ArrayList<Hashtable<String, Object>>();
		while(it.hasNext()) {
			Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
			Object[] listElement = it.next().getValues();
			String colName;
			Double value;
			
			if(seriesIdx != -1) {
				name = listElement[seriesIdx].toString();
			}
			
			elementHash.put("series", name);
			elementHash.put("label", listElement[labelIdx]);
			elementHash.put("lat", listElement[latIdx]);
			elementHash.put("lon", listElement[lonIdx]);
			if(sizeIdx != -1) {
				elementHash.put("size", listElement[sizeIdx]);
			}
			if (timeIdx != -1) {
				elementHash.put("time", listElement[timeIdx]);
			}
			
			allData.add(elementHash);
		}
		allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", allData);
		allHash.put("lat", names[latIdx]);
		allHash.put("lon", names[lonIdx]);
		if (sizeIdx != -1) {
			allHash.put("size", names[sizeIdx]);
		}
		else {
			allHash.put("size", "");
		}
		allHash.put("locationName", names[labelIdx]);
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
		String[] qsNames = dataFrame.getQsHeaders();
		String[] frameNames = dataFrame.getColumnHeaders();
		
		int offset = 0;
		if(!dataFrame.isNumeric(qsNames[1])) {
			alignHash.put(seriesString, frameNames[0]);
			offset = 1;
		}
		alignHash.put(labelString, frameNames[0 + offset]);
		alignHash.put(latString, frameNames[1 + offset]);
		if(frameNames.length > 2 + offset)
			alignHash.put(lonString, frameNames[2 + offset]);
		if(frameNames.length > 3 + offset)
			alignHash.put(sizeString, frameNames[3 + offset]);
		if(frameNames.length > 4 + offset)
			alignHash.put(seriesString, frameNames[4 + offset]);
		if(frameNames.length > 5 + offset)
			alignHash.put(timeString, frameNames[5+ offset]);
		return alignHash;
	}
	
	/**
	 * Already performed by getAlignHash; unnecessary?
	 * @return String Array of Table headers
	 */
	public String[] getVariableArray() {
		return this.dataFrame.getColumnHeaders();
	}
	
	@Override
	/**
	 * Converts a given Hashtable to a Json and passes it to the browser. Deprecated?
	 * @param table Hashtable - the correctly formatted data from the SPARQL query results.
	 */
	public void callIt(final Hashtable table) {
		output = table;
		Gson gson = new Gson();
		logger.info("Converted gson");
//		JSValue val = browser.executeJavaScriptAndReturnValue("start('" + gson.toJson(table) + "');");
		output.clear();
		data.clear();
		allHash.clear();
	}
}
