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
import java.util.LinkedHashMap;

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
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/parcoords.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Processed text and numerical data accordingly for the parallel coordinates visualization.*/
	public Hashtable processQueryData()
	{
		ArrayList dataArrayList = new ArrayList();
		String[] var = wrapper.getVariables(); 		
		for (int i=0; i<list.size(); i++)
		{	
			LinkedHashMap elementHash = new LinkedHashMap();
			Object[] listElement = list.get(i);
			String colName;
			Double value;
			for (int j = 0; j < var.length; j++) 
			{	
				colName = var[j];
				if (listElement[j] instanceof String)
				{	
					String text = (String) listElement[j];
					text = text.replaceAll("^\"|\"$", "");
					if (text.length() >= 30) {
					text = text.substring(0, Math.min(text.length(), 30));  //temporary
					text = text + "...";
					}
					elementHash.put(colName, text);
				}
				else 
				{	
					value = (Double) listElement[j];	
					elementHash.put(colName, value);
				}
			}	
				dataArrayList.add(elementHash);			
		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataArrayList);
		
		return allHash;
	}
	
}
