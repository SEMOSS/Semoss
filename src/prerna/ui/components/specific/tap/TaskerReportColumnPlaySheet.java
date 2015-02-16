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
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class TaskerReportColumnPlaySheet extends BrowserPlaySheet{

	/**
	 * Constructor for ColumnChart.
	 */
	public TaskerReportColumnPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechart.html";
	}

	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.

	 * @return Hashtable Includes the data series, graph title, and the x- and y-axis titles.*/
	public Hashtable processQueryData()
	{
		Hashtable allHash = new Hashtable();
		//assume only one return
		Object[] listElement = list.get(0);
		String serviceName = (String) listElement[0];
		//assume three numbers to report
		
		String[] var = wrapper.getVariables();
		String[] seriesName = new String[3];
		seriesName[0]=var[1];
		seriesName[1]=var[2];
		seriesName[2]=var[3];
		Hashtable colorHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		
		double[] series = new double[1];
		series[0]=(Double) listElement[1];
		seriesHash.put(var[1], series);
		colorHash.put(var[1], "#44e378");
		series = new double[1];
		series[0]=(Double) listElement[2];
		seriesHash.put(var[2], series);
		colorHash.put(var[2], "#3e9cec ");
		series = new double[1];
		series[0]=(Double) listElement[3];
		seriesHash.put(var[3], series);
		colorHash.put(var[3], "#ec3e3e");

		allHash.put("type",  "column");
		allHash.put("title",  serviceName+" Tasker Completion Status");
		allHash.put("yAxisTitle", "Count of Taskers");
		allHash.put("xAxisTitle", "Tasker Status");
		String[] name = new String[1];
		name[0]="";
		allHash.put("xAxis", name);

		allHash.put("dataSeries",  seriesHash);
		allHash.put("colorSeries", colorHash);
		
		return allHash;
	}

}
