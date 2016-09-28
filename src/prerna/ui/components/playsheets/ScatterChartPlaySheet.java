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
import java.util.Map;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * The GridScatterPlaySheet class creates the panel and table for a scatter plot view of data from a SPARQL query.
 */
public class ScatterChartPlaySheet extends BrowserPlaySheet{
	
	private static final String labelString = "label";
	private static final String seriesString = "series";
	private static final String xString = "x";
	private static final String yString = "y";
	private static final String zString = "z";
	private static final String heatString = "heat";
	
	/**
	 * Constructor for GridScatterSheet.
	 */
	public ScatterChartPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		//fileName  = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html";
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatterplot.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable Includes the data series, graph title, and the x- and y-axis titles.*/
	public void processQueryData()
	{
		String[] names = dataFrame.getColumnHeaders();

		Map<String, String> align = getDataTableAlign();
		int labelIdx = -1;
		int seriesIdx = -1;
		int xIdx = -1;
		int yIdx = -1;
		int zIdx = -1;
		int heatIdx = -1;
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
				else if(key.contains(xString)){
					xIdx = i;
				}
				else if(key.contains(yString)){
					yIdx = i;
				}
				else if(key.contains(zString)){
					zIdx = i;
				}
				else if(key.contains(heatString)){
					heatIdx = i;
				}
			}
		}

		String name = names[labelIdx];
		
		Iterator<Object[]> it = dataFrame.iterator();
		ArrayList<Hashtable<String, Object>> allData = new ArrayList<Hashtable<String, Object>>();
		while(it.hasNext())
		{
			Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
			Object[] listElement = it.next();
			
			if(seriesIdx != -1) {
				name = listElement[seriesIdx].toString();
			}
			
			elementHash.put("series", name);
			elementHash.put("label", listElement[labelIdx]);
			elementHash.put("x", listElement[xIdx]);
			if(yIdx != -1)
				elementHash.put("y", listElement[yIdx]);
			if(zIdx != -1)
				elementHash.put("z", listElement[zIdx]);
			if(heatIdx != -1)
				elementHash.put("heat", listElement[heatIdx]);
			
			allData.add(elementHash);
		}
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", allData);
		allHash.put("title",  names[xIdx] + " vs " + names[yIdx]);
		allHash.put("labelHeader", names[labelIdx]);
		allHash.put("xAxisTitle", names[xIdx]);
		if(yIdx != -1)
			allHash.put("yAxisTitle", names[yIdx]);
		if(zIdx != -1)
			allHash.put("zAxisTitle", names[zIdx]);

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

		int offset = 0;
		if(!dataFrame.isNumeric(names[1])) {
			alignHash.put(seriesString, names[0]);
			offset = 1;
		}
		
		alignHash.put(labelString, names[0 + offset]);
		alignHash.put(xString, names[1 + offset]);
		if(names.length > 2 + offset)
			alignHash.put(yString, names[2 + offset]);
		if(names.length > 3 + offset)
			alignHash.put(zString, names[3 + offset]);
		if(offset == 0 && names.length > 4)
			alignHash.put(heatString, names[4 + offset]);
		return alignHash;
	}
	
}
