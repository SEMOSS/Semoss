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
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class is used to create a playsheet for a grid scatter with lines.
 */
public class GridScatterWithLinesSheet extends BrowserPlaySheet{
	
	/**
	 * Constructor for GridScatterWithLinesSheet.
	 */
	public GridScatterWithLinesSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html";
	}
	
	/**
	 * Processes the query data necessary to create the grid scatter playsheet.
	
	 * @return Hashtable<String,Hashtable>	Hashtable containing information about values for the axes, titles, and legends for the display. */
	public Hashtable<String,Hashtable> processQueryData()
	{
		Hashtable<String,Object[][]> dataHash = new Hashtable<String,Object[][]>();
		Object[][] dataSeries = new Object[list.size()][4];
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			Object[] dataSet = new Object[4];
			dataSet[0]=(Double) listElement[1];
			dataSet[1]=(Double) listElement[2];
			if (listElement.length<4)
			{
				dataSet[2]=0.0;
			}
			dataSet[2]=(Double) listElement[3];
			dataSet[3]=(String) listElement[0];
			dataSeries[i]=dataSet;
		}

		dataHash.put("Series", dataSeries);
		
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("type",  "bubble");
		String[] var = wrapper.getVariables();
		allHash.put("title",  var[1] + " vs " + var[2]);
		allHash.put("xAxisTitle", var[1]);
		allHash.put("yAxisTitle", var[2]);
		allHash.put("zAxisTitle", var[3]);
		
		allHash.put("xAxisMin", 0);
		allHash.put("xAxisMax", 10);
		allHash.put("yAxisMin", 0);
		allHash.put("yAxisMax", 10);

		allHash.put("xLineLabel","true");
		allHash.put("yLineLabel","true");
		
		Integer[] lineValues = {0,3,7,10};
		allHash.put("xLineVal",lineValues);
		allHash.put("yLineVal",lineValues);

		String[] lineLegend = {"Does not Support","Supports with Customization", "Supports with Configuration", "Supports out of Box"};
		allHash.put("xLineLegend",lineLegend);
		allHash.put("yLineLegend",lineLegend);

		return allHash;
	}
	
}
