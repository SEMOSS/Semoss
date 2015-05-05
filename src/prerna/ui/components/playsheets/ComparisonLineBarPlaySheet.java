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
import java.util.Collection;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.GraphDataModel;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ComparisonLineBarPlaySheet extends ColumnChartPlaySheet{

	private static final Logger logger = LogManager.getLogger(ComparisonLineBarPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	
	public ComparisonLineBarPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, ArrayList<Hashtable<String, Object>>> barObj = new Hashtable<String, ArrayList<Hashtable<String, Object>>>();
		Hashtable<String, ArrayList<Hashtable<String, Object>>> lineObj = new Hashtable<String, ArrayList<Hashtable<String, Object>>>();
		// format of the return will be ?barSeriesName ?yBarVal ?line1SeriesName ?yLine1Val ?line2SeriesName ?yLine2Val ?xValue etc.
		int lastCol = names.length - 1 ;
		ArrayList<String> usedList = new ArrayList<String>();
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int seriesVal = 1; seriesVal <= elemValues.length / 2; seriesVal++)
			{
				

				int firstCol = (seriesVal - 1) * 2;
				String xVal = elemValues[lastCol].toString();
				String seriesName = elemValues[firstCol].toString();
				
				String usedKey = xVal + seriesName;
				
				if(!usedList.contains(usedKey)){
					usedList.add(usedKey);
					
					// get the right hashtable to fill
					Hashtable<String, ArrayList<Hashtable<String, Object>>> fillHash = lineObj;
					if(seriesVal==1) 
						fillHash = barObj;
					
					
					ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
					
					if(fillHash.containsKey(seriesName))
						seriesArray = fillHash.get(seriesName);
					else
						fillHash.put(seriesName, seriesArray);
					
					Hashtable<String, Object> elementHash = new Hashtable();
					elementHash.put("x",xVal);
					elementHash.put("y", elemValues[firstCol+1]);
					elementHash.put("seriesName",  seriesName);
					seriesArray.add(elementHash);
				}
			}
		}
		Hashtable<String, Collection<ArrayList<Hashtable<String, Object>>>> dataObj = new Hashtable<String, Collection<ArrayList<Hashtable<String, Object>>>>();
		dataObj.put("bar", barObj.values());
		dataObj.put("line", lineObj.values());
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
}
