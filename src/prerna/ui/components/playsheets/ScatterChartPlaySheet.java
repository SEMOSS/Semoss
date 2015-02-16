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

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The GridScatterPlaySheet class creates the panel and table for a scatter plot view of data from a SPARQL query.
 */
public class ScatterChartPlaySheet extends BrowserPlaySheet{
	
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
	public Hashtable processQueryData()
	{
		ArrayList allData = new ArrayList();
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);
			
			elementHash.put("series", names[0]);
			elementHash.put("label", listElement[0]);
			elementHash.put("x", listElement[1]);
			if(listElement.length>2)
				elementHash.put("y", listElement[2]);
			if(listElement.length>3)
				elementHash.put("z", listElement[3]);
//			
//			Object[] dataSet = new Object[4];
//			dataSet[0]=(Double) listElement[1];
//			dataSet[1]=(Double) listElement[2];
//			if (listElement.length<4)
//			{
//				dataSet[2]=0.0;
//			}
//			else
//			{
//			dataSet[2]=(Double) listElement[3];
//			}
//			dataSet[3]=(String) listElement[0];
			allData.add(elementHash);
		}
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", allData);
		allHash.put("title",  names[1] + " vs " + names[2]);
		allHash.put("xAxisTitle", names[1]);
		if(names.length>2)
			allHash.put("yAxisTitle", names[2]);
		if(names.length>3)
			allHash.put("zAxisTitle", names[3]);
		return allHash;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		alignHash.put("label", names[0]);
		alignHash.put("x", names[1]);
		if(names.length>2)
			alignHash.put("y", names[2]);
		if(names.length>3)
			alignHash.put("z", names[3]);
		return alignHash;
	}
	
}
