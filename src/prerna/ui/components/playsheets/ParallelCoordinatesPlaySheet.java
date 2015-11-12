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

/**
 * The Play Sheet for creating a Parallel Coordinates diagram.
 */
public class ParallelCoordinatesPlaySheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for ParallelCoordinatesPlaySheet.
	 */
	public ParallelCoordinatesPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/parcoords.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Processed text and numerical data accordingly for the parallel coordinates visualization.*/
	public void processQueryData()
	{
		ArrayList<Map<String, Object>> dataArrayList = new ArrayList<Map<String, Object>>();
		String[] var = dataFrame.getColumnHeaders();
		
		Iterator<Object[]> it = dataFrame.iterator(true);
		while(it.hasNext())
		{	
			LinkedHashMap<String, Object> elementHash = new LinkedHashMap<String, Object>();
			Object[] listElement = it.next();
			String colName;
			Double value;
			for (int j = 0; j < var.length; j++) 
			{	
				colName = var[j];
				if (listElement[j] instanceof String)
				{	
					String text = (String) listElement[j];
					text = text.replaceAll("^\"|\"$", "");
					elementHash.put(colName, text);
				}
				else if (listElement[j] instanceof Double)
				{	
					value = (Double) listElement[j];	
					elementHash.put(colName, value);
				} 
				else {
					elementHash.put(colName, listElement[j]);
				}
			}	
				dataArrayList.add(elementHash);			
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", dataArrayList);
		
		this.dataHash = allHash;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		String[] names = dataFrame.getColumnHeaders();
		for(int namesIdx = 0; namesIdx<names.length; namesIdx++){
			alignHash.put("dim " + namesIdx, names[namesIdx]);
		}
		return alignHash;
	}
	
}
