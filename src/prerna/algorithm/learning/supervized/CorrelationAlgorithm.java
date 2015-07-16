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
package prerna.algorithm.learning.supervized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.math.BarChart;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;

public class CorrelationAlgorithm implements IAnalyticRoutine {

	//TODO: this approach was never finished!!!
	private static final Logger LOGGER = LogManager.getLogger(CorrelationAlgorithm.class.getName());
	
	private ITableDataFrame dataFrame;
	private String[] names;
	private boolean[] isNumeric;
	
	private ArrayList<String[]> valuesList = new ArrayList<String[]>();
	private ArrayList<String[]> uniqueValuesList = new ArrayList<String[]>();
	private ArrayList<int[]> uniqueValueCountsList = new ArrayList<int[]>();
	
	private double[] standardDev;
	private double[][] covariance;
	private double[][] correlation;
	
	public CorrelationAlgorithm() {

	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		dataFrame = data[0];
		this.names = dataFrame.getColumnHeaders();
		this.isNumeric = dataFrame.isNumeric();
		
		int i;
		int j;
		int namesLength = names.length;
		int numVariables = namesLength - 1;
		
		//go through all variables and get the outputs for each		
		for(i = 1; i < namesLength; i++) {
			String[] values;
			String[] uniqueValues;
			int[] uniqueValueCount;
			
			//calculate the number of times each value corresponds to one of the instances
			if(!isNumeric[i]) {
				Object[] valuesObj = dataFrame.getColumn(names[i]);
				String[] valuesArr = ArrayUtilityMethods.convertObjArrToStringArr(valuesObj);
				BarChart chart = new BarChart(valuesArr);
				chart.calculateCategoricalBins("?", true, true);
				values = chart.getStringValues();
				uniqueValues = chart.getStringUniqueValues();
				uniqueValueCount = chart.getStringUniqueCounts();
				
			}else {
				Object[] valuesObj = dataFrame.getColumn(names[i]);
				double[] valuesArr = ArrayUtilityMethods.convertObjArrToDoubleArr(valuesObj);
				BarChart chart = new BarChart(valuesArr);
				if(chart.isUseCategoricalForNumericInput()) {
					chart.calculateCategoricalBins("?", true, true);
					values = chart.getStringValues();
					uniqueValues = chart.getStringUniqueValues();
					uniqueValueCount = chart.getStringUniqueCounts();
				} else {
					values = chart.getAssignmentForEachObject();
					uniqueValues = chart.getNumericalBinOrder();
					uniqueValueCount = chart.getNumericBinCounterArr();
				}		
			}

			valuesList.add(values);
			uniqueValuesList.add(uniqueValues);
			uniqueValueCountsList.add(uniqueValueCount);
		
		}
		
		//calculate the standard deviation for each column
		standardDev = new double[numVariables];
		for(i=0; i<numVariables; i++) {
			standardDev[i] = Math.sqrt(calculateVariance(i));
			System.out.println(names[i+1]+" std dev : "+standardDev[i]);
		}

		//calculate the covariance for each pair of columns
		covariance = new double[numVariables][numVariables];
		for(i=0; i<numVariables; i++) {
			for(j=0; j<numVariables; j++) {
				if(i==j)
					covariance[i][j] = Math.pow(standardDev[i],2);
				else
					covariance[i][j] = calculateCovariance(i,j);
			}
		}
		
		//calculate the correlation for each pair of columns
		correlation = new double[numVariables][numVariables];
		for(i=0; i<numVariables; i++) {
			for(j=0; j<numVariables; j++) {
				correlation[i][j] = covariance[i][j] / (standardDev[i] * standardDev[j]);
			}
		}
				
		return null;
	}
	
	/**
	 * Calculate the variance in column x
	 * @param x
	 * @return
	 */
	private double calculateVariance(int x) {
		
		int i;
		double numInstances = dataFrame.getNumRows()*1.0;
		int[] uniqueValueCounts = uniqueValueCountsList.get(x);
		int numUniqueValueCounts = uniqueValueCounts.length;
		
		double variance = 0.0;
		for(i=0; i<numUniqueValueCounts; i++) {
			variance += Math.pow(1 - uniqueValueCounts[i] / numInstances,2) * (uniqueValueCounts[i] / numInstances);
		}

		return variance;
	}
	
	/**
	 * Calculate the covariance between two columns, x and y
	 * @param x
	 * @param y
	 * @return
	 */
	private double calculateCovariance(int x, int y) {
		
		int i;
		int j;
		double numInstances = dataFrame.getNumRows()*1.0;
		int[] xUniqueValueCounts = uniqueValueCountsList.get(x);
		int[] yUniqueValueCounts = uniqueValueCountsList.get(y);
		int numXUniqueValueCounts = xUniqueValueCounts.length;
		int numYUniqueValueCounts = yUniqueValueCounts.length;
		
		double[][] occurenceArr = calculateOccurence(x,y);
		
		double covariance = 0.0;
		for(i=0; i<numXUniqueValueCounts; i++) {
			for(j=0; j<numYUniqueValueCounts; j++) {
				covariance += (1 - xUniqueValueCounts[i] / numInstances) * (1 - yUniqueValueCounts[j] / numInstances) * (occurenceArr[i][j] / numInstances);
			}
		}

		return covariance;
	}
	
	private double[][] calculateOccurence(int x, int y) {
		int numInstances = dataFrame.getNumRows();
		String[] xValues = valuesList.get(x);
		String[] yValues = valuesList.get(y);
		if(numInstances!=xValues.length || numInstances !=yValues.length) {
			LOGGER.error("Variables do not have the same number of instances");
			return null;
		}
		
		String[] xUniqueValues = uniqueValuesList.get(x);
		String[] yUniqueValues = uniqueValuesList.get(y);

		int i;
		int j;
		int p;
		int numXUniqueValues = xUniqueValues.length;
		int numYUniqueValues = yUniqueValues.length;

		double[][] probArr = new double[numXUniqueValues][numYUniqueValues];
		for(i=0; i<numXUniqueValues; i++) {
			for(j=0; j<numYUniqueValues; j++) {
				probArr[i][j] = 0;
			}
		}
		
		for(p=0; p<numInstances; p++) {
			String xName = xValues[p];
			String yName = yValues[p];
			
			INNER : for(i=0; i<numXUniqueValues; i++) {
				if(xName.equals(xUniqueValues[i])) {
					
					for(j=0; j<numYUniqueValues; j++) {
						if(yName.equals(yUniqueValues[j])) {

							probArr[i][j] ++;
							break INNER;
							
						}
					}
				}
			}
		}

		return probArr;
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDefaultViz() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public double[] getStandardDev() {
		return standardDev;
	}

	public double[][] getCovariance() {
		return covariance;
	}

	public double[][] getCorrelation() {
		return correlation;
	}

}