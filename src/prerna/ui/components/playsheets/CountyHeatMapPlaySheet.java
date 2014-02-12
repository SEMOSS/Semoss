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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

/**
 * The Play Sheet for the United States geo-location data heatmap.  
 * Visualizes a world heat map that can show any numeric property on a node.
 */
public class CountyHeatMapPlaySheet extends BrowserPlaySheet {

	/**
	 * Constructor for USHeatMapPlaySheet.
	 */
	public CountyHeatMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/countyheatmap.html";
	}
	
	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an appropriate format for the specific play sheet.	
	 * @return Hashtable - A Hashtable of the queried data to be converted into json format.  
	 */
	public Hashtable processQueryData()
	{
		HashSet data = new HashSet();
		String[] var = wrapper.getVariables(); 	
		
		//Possibly filter out all US Facilities from the query?
		
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
					elementHash.put(colName, Integer.parseInt(text.replaceAll("\"","")));
				}
				else 
				{	
					value = (Double) listElement[j];							
					elementHash.put(colName, value);

				}
						
			}	
			data.add(elementHash);		
		}

		    
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", data);
		

		allHash.put("value", var[1]);
		allHash.put("locationName", var[0]);
		
		
		return allHash;
	}
}
