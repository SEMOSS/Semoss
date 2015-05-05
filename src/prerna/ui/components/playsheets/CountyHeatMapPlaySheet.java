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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import prerna.engine.api.ISelectStatement;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURIImpl;

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
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/countyheatmap.html";
	}
	
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.
	
	 * @return Object - results with given URI.*/
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getRawVar(varName);
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
				if(listElement[j] instanceof BigdataURIImpl && j==0)
				{
					BigdataURIImpl val = (BigdataURIImpl) listElement[j];
					String text = val.toString();
					text = text.substring(text.lastIndexOf("/")+1);
					elementHash.put(colName, text.replaceAll("\"",""));
				}
				else if(listElement[j] instanceof BigdataURIImpl && j==1)
				{
					BigdataURIImpl val = (BigdataURIImpl) listElement[j];
					String text = val.toString();
					text = text.substring(text.lastIndexOf("/")+1);
					elementHash.put(colName, text.replaceAll("\"",""));
				}
				else if(listElement[j] instanceof BigdataURIImpl && j>1)
				{
					BigdataURIImpl val = (BigdataURIImpl) listElement[j];
					String text = val.toString();
					text = text.substring(text.lastIndexOf("/")+1);
					elementHash.put(colName, text.replaceAll("\"",""));
				}
				else if(listElement[j] instanceof BigdataLiteral && j==1)
				{
					BigdataLiteral val = (BigdataLiteral) listElement[j];
					if(val.stringValue().contains("NaN"))
						elementHash.put(colName, 0.0);
					else{
						Object numVal = val.doubleValue();
						if(numVal==null)
							numVal = val.floatValue();
						if(numVal==null)
							numVal = val.integerValue();
						elementHash.put(colName, numVal);
					}
				}
				else if(listElement[j] instanceof BigdataLiteral && j>1)
				{
					BigdataLiteral val = (BigdataLiteral) listElement[j];
					elementHash.put(colName, val.stringValue().replaceAll("\"",""));
				}
				else if (listElement[j] instanceof String && j==1) //dont think we use this
				{	
					String text = (String) listElement[j];
					text = text.substring(text.indexOf("\""), text.indexOf("\""));
					elementHash.put(colName, Double.parseDouble(text.replaceAll("\"","")));
				}
				else if(listElement[j] instanceof String)//dont think we use this
				{
					String text = (String) listElement[j];
					text = text.substring(text.lastIndexOf("\\"));
					elementHash.put(colName, text.replaceAll("\"",""));

				}
				else //dont think we use this
				{	
					value = (Double) listElement[j];							
					elementHash.put(colName, value);
				}
						
			}
			data.add(elementHash);		
		}
		    
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", data);
		allHash.put("locationName", var[0]);
		allHash.put("value", var[1]);
		ArrayList<String> propertyName = new ArrayList<String>();
		for(int i=0;i<var.length-2;i++)
			propertyName.add(var[i+2]);
		allHash.put("propertyNames",propertyName);
		
		return allHash;
	}
}
