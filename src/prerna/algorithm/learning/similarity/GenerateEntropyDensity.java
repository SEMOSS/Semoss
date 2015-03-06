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
package prerna.algorithm.learning.similarity;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.AlgorithmDataFormatter;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class GenerateEntropyDensity {
	
	private Object[][] data;
	private double[] entropyDensityArray;
	private String[] columnTypes;
	private boolean includeLastColumn = false;
	
	public double[] getEntropyDensityArray() {
		return entropyDensityArray;
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData) {
		data = AlgorithmDataFormatter.manipulateValues(queryData, false);
		columnTypes = AlgorithmDataFormatter.determineColumnTypes(queryData);
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData, boolean includeLastColumn) {
		this.includeLastColumn = includeLastColumn;
		data = AlgorithmDataFormatter.manipulateValues(queryData, includeLastColumn);
		columnTypes = AlgorithmDataFormatter.determineColumnTypes(queryData);
	}
	
	public double[] generateEntropy() {
		int i;
		int size;
		if(includeLastColumn) {
			size = data.length;
		}else {
			size = data.length - 1;
		}
		entropyDensityArray = new double[size];

		for(i = 0; i < size; i++) {
			Object[] objDataRow = data[i];
			Hashtable<String, Object>[] binData = null;
			if(columnTypes[i].equals("STRING")) {
				//TODO: shouldn't create chart data, should use CalculateEntropy.java class
				String[] dataRow = ArrayUtilityMethods.convertObjArrToStringArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				chart.calculateCategoricalBins("?", true, true);
				chart.generateJSONHashtableCategorical();
				binData = chart.getRetHashForJSON();
			} else {
				Double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				if(chart.isUseCategoricalForNumericInput()) {
					chart.calculateCategoricalBins("?", true, true);
					chart.generateJSONHashtableCategorical();
					binData = chart.getRetHashForJSON();
				} else {
					chart.generateJSONHashtableNumerical();
					binData = chart.getRetHashForJSON();
				}
			}
			
			int j;
			int numBins = binData.length;
			if(numBins > 1) {
				int[] values = new int[numBins];
				for(j = 0; j < numBins; j++) {
					values[j] = (int) binData[j].get("y");
				}
				entropyDensityArray[i] = StatisticsUtilityMethods.calculateEntropyDensity(values);
			}
		}
		
		return entropyDensityArray;
	}
	
	public String[] getColumnTypes() {
		return columnTypes;
	}
	
}
