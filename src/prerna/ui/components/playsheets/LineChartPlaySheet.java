/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.Iterator;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class LineChartPlaySheet extends BrowserPlaySheet{

	public LineChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/c3linechart.html";
	}	
	
	public void processQueryData()
	{
		ArrayList<Hashtable<String, Object>> dataObj = new ArrayList<Hashtable<String, Object>>();
		String[] names = dataFrame.getColumnHeaders();

		Iterator<Object[]> it = dataFrame.iterator(true);
		while(it.hasNext()) {
			Object[] elemValues = it.next();
			
			Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
			elementHash.put("xAxis",  "");
			elementHash.put("uriString",  elemValues[0].toString());
			for(int j = 1; j < elemValues.length; j++){
				elementHash.put(names[j], elemValues[j]);
			}
			dataObj.add(elementHash);
		}
		
		Hashtable<String, Object> lineChartHash = new Hashtable<String, Object>();
		lineChartHash.put("names", names);
		lineChartHash.put("dataSeries", dataObj);
		lineChartHash.put("type", "line");
		
		this.dataHash = lineChartHash;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		String[] names = dataFrame.getColumnHeaders();
		
		alignHash.put("label", names[0]);
		for(int namesIdx = 1; namesIdx<names.length; namesIdx++){
			alignHash.put("value " + namesIdx, names[namesIdx]);
		}
		return alignHash;
	}
}

