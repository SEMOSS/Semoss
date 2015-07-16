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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The Play Sheet for creating a Parallel Coordinates diagram.
 */
public class GanttPlaySheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for ParallelCoordinatesPlaySheet.
	 */
	public GanttPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/gantt.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Processed text and numerical data accordingly for the parallel coordinates visualization.*/
	public void processQueryData()
	{
		String taskNameString = "taskName";
		ArrayList dataArrayList = new ArrayList();
		ArrayList taskNames = new ArrayList();
		
		String[] var = dataFrame.getColumnHeaders();
		Iterator<Object[]> it = dataFrame.iterator(true, null);
		while(it.hasNext())
		{	
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = it.next();
			String colName;
			Double value;
			String elementConcatName = "";
			String propConcatName = "";
			
			for (int j = 0; j < var.length; j++) 
			{	
				colName = var[j];
				
				
				
				if(j == 0 || j == 1)
				{
//					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//					String stringDate = (String) listElement[j];
//					Date date = null;
					
					String date;
					
//					try {
//						date = df.parse(stringDate);
						date = (String) listElement[j];
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					
					elementHash.put(colName, date);
				}
				else if (j == var.length - 1)
				{	
//					String propToConcat = colName;
//					propToConcat = propToConcat.replaceAll("^\"|\"$", "");
//					propConcatName = propConcatName + propToConcat;

					String elementToConcat = (String) listElement[j];
					elementToConcat = elementToConcat.replaceAll("^\"|\"$", "");
					elementConcatName = elementConcatName + elementToConcat;
					
					elementHash.put(taskNameString, elementConcatName);
					taskNames.add(elementConcatName);
				}
				else 
				{	
//					String propToConcat = colName;
//					propToConcat = propToConcat.replaceAll("^\"|\"$", "");
//					propConcatName = propConcatName + propToConcat + ", ";

					String elementToConcat = (String) listElement[j];
					elementToConcat = elementToConcat.replaceAll("^\"|\"$", "");
					elementConcatName = elementConcatName + elementToConcat + ", ";				
				}
			}	
				dataArrayList.add(elementHash);			
		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataArrayList);
		java.util.Collections.sort(taskNames);
		allHash.put("dataKeys", taskNames);

		this.dataHash = allHash;
	}
	
}
