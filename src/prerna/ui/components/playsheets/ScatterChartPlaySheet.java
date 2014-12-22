/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
		String[] var = wrapper.getVariables();
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);
			
			elementHash.put("series", var[0]);
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
		allHash.put("title",  var[1] + " vs " + var[2]);
		allHash.put("xAxisTitle", var[1]);
		if(var.length>2)
			allHash.put("yAxisTitle", var[2]);
		if(var.length>3)
			allHash.put("zAxisTitle", var[3]);
		return allHash;
	}
	
}
