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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class GenerateEntropyDensity {
	
	private ArrayList<Object[]> queryData;
	private Object[][] data;
	private double[] entropyDensityArray;
	private boolean[] isCategorical;
	
	public double[] getEntropyDensityArray() {
		return entropyDensityArray;
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData) {
		this.queryData = queryData;
		manipulateValues();
	}
	
	//TODO: get to work with nulls/missing data
	private void manipulateValues() {
		int counter = 0;
		
		int numProps = queryData.get(0).length;
		data = new Object[numProps-1][queryData.size()];
		isCategorical = new boolean[numProps];
		Object[][] trackType = new Object[numProps][queryData.size()];
		
		int i;
		int size = queryData.size();
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData.get(i);
			int j;
			for(j = 1; j < numProps - 1; j++) {
				data[j][counter] = dataRow[j];
				trackType[j][counter] = Utility.processType(dataRow[j].toString());
			}
			counter++;
		}
		
		for(i = 1; i < numProps - 1; i++) {
			int j;
			int stringCounter = 0;
			int doubleCounter = 0;
			for(j = 0; j < counter; j++) {
				if(trackType[i][j].toString().equals("STRING")) {
					stringCounter++;
				} else {
					doubleCounter++;
				}
			}
			if(stringCounter > doubleCounter) {
				isCategorical[i] = true;
			}
		}
	}
	
	public double[] generateEntropy() {
		int i;
		int size = data.length;
		entropyDensityArray = new double[size - 1];
		
		for(i = 1; i < size; i++) {
			Object[] objDataRow = data[i];
			Hashtable<String, Object>[] binData = null;
			if(isCategorical[i]) {
				String[] dataRow = ArrayUtilityMethods.convertObjArrToStringArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			} else {
				double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleArr(objDataRow);
				Arrays.sort(dataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			}
			
			int j;
			int numBins = binData.length;
			int[] values = new int[numBins];
			for(j = 0; j < numBins; j++) {
				values[j] = (int) binData[j].get("y");
			}
			
			entropyDensityArray[i-1] = StatisticsUtilityMethods.calculateEntropyDensity(values);
		}
		
		return entropyDensityArray;
	}
}
