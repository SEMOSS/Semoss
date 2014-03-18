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
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The GridScatterPlaySheet class creates the panel and table for a scatter plot view of data from a SPARQL query.
 */
public class GridScatterSheet extends BrowserPlaySheet{
	
	/**
	 * Constructor for GridScatterSheet.
	 */
	public GridScatterSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable Includes the data series, graph title, and the x- and y-axis titles.*/
	public Hashtable processQueryData()
	{
		Hashtable dataHash = new Hashtable();
		ArrayList allData = new ArrayList();
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);
			Object[] dataSet = new Object[4];
			dataSet[0]=(Double) listElement[1];
			dataSet[1]=(Double) listElement[2];
			if (listElement.length<4)
			{
				dataSet[2]=0.0;
			}
			else
			{
			dataSet[2]=(Double) listElement[3];
			}
			dataSet[3]=(String) listElement[0];
			allData.add(dataSet);
		}
		Object[][] dataSeries = new Object[allData.size()][4];
		for(int i=0; i< allData.size();i++)
		{
			dataSeries[i]=(Object[]) allData.get(i);
		}
		dataHash.put("Series", dataSeries);
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("type",  "scatter");
		String[] var = wrapper.getVariables();
		allHash.put("title",  var[1] + " vs " + var[2]);
		allHash.put("xAxisTitle", var[1]);
		allHash.put("yAxisTitle", var[2]);
		return allHash;
	}
	
}
