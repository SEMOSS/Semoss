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
import java.util.Hashtable;
import java.util.List;

import prerna.algorithm.impl.AlgorithmDataFormatter;
import prerna.math.BarChart;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class BinnedPieChartPlaySheet extends BrowserPlaySheet{

	public BinnedPieChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/piechart.html";
	}
	
	
	public void processQueryData()
	{	
		List<Object[]> list = dataFrame.getData();
		String[] names = dataFrame.getColumnHeaders();
		
		Object[][] data = AlgorithmDataFormatter.manipulateValues(list,true);	
		String[] columnTypes = AlgorithmDataFormatter.determineColumnTypes(list);
		
		Object[] objDataRow = data[1];
		String[] uniqueValues;
		String labels;
		int[] uniqueCounts;
		
		if(columnTypes[1].equals(AlgorithmDataFormatter.STRING_KEY)) {
			
			String[] dataRow = ArrayUtilityMethods.convertObjArrToStringArr(objDataRow);
			BarChart chart = new BarChart(dataRow);
			chart.calculateCategoricalBins("?", true, true);
			
			uniqueValues = chart.getStringUniqueValues();
			labels = chart.getCategoricalLabel();
			uniqueCounts = chart.getStringUniqueCounts();
			
		}else {
			
			Double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(objDataRow);
			BarChart chart = new BarChart(dataRow);

			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("?", true, true);
				
				uniqueValues = chart.getStringUniqueValues();
				labels = chart.getCategoricalLabel();
				uniqueCounts = chart.getStringUniqueCounts();

			} else {

				uniqueValues = chart.getNumericalBinOrder();
				labels = chart.getNumericalLabel();
				uniqueCounts = chart.getNumericBinCounterArr();

			}
		}

		int j;		
		int numBins = uniqueValues.length;
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();

		for(j=0; j < numBins; j++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("pieCat", uniqueValues[j]);
			innerHash.put("pieVal", uniqueCounts[j]);
			seriesArray.add(innerHash);
		}
		dataObj.add(seriesArray);
		
		names = new String[]{names[1],labels};
		
		Hashtable<String, Object> pieChartHash = new Hashtable<String, Object>();
		pieChartHash.put("names", names);
		pieChartHash.put("type", "pie");
		pieChartHash.put("dataSeries", dataObj);
		
		this.dataHash = pieChartHash;
	}
}
