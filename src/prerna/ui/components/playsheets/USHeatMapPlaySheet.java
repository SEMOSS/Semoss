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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The Play Sheet for the United States geo-location data heatmap.  
 * Visualizes a world heat map that can show any numeric property on a node.
 */
public class USHeatMapPlaySheet extends BrowserPlaySheet {

	/**
	 * Constructor for USHeatMapPlaySheet.
	 */
	public USHeatMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/usheatmap.html";
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
					elementHash.put(colName, text.replaceAll("_"," "));
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
