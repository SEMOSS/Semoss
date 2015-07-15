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
package prerna.math;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;

public final class SimilarityWeighting {

	private SimilarityWeighting() {
		
	}
	
	public static void calculateWeights(ITableDataFrame dataFrame, int instanceIndex, String[] attributeNames, boolean[] isNumeric, Map<String, Double> numericalWeights, Map<String, Double> categoricalWeights) {
		int i = 0;
		int size = attributeNames.length;
		String instanceType = attributeNames[instanceIndex];
		
		List<Double> numericalEntropy = new ArrayList<Double>();
		List<String> numericalNames = new ArrayList<String>();
		
		List<Double> categoricalEntropy = new ArrayList<Double>();
		List<String> categoricalNames = new ArrayList<String>();
		
		for(; i < size; i++) {
			String attribute = attributeNames[i];
			if(attribute.equals(instanceType)) {
				continue;
			}
			if(isNumeric[i]) {
				numericalNames.add(attribute);
				numericalEntropy.add(dataFrame.getEntropyDensity(attribute));
			} else {
				categoricalNames.add(attribute);
				categoricalEntropy.add(dataFrame.getEntropyDensity(attribute));
			}
		}
		
		if(!numericalEntropy.isEmpty()){
			double[] numericalWeightsArr = generateWeighting(numericalEntropy.toArray(new Double[0]));
			i = 0;
			int numNumeric = numericalNames.size();
			for(; i < numNumeric; i++) {
				numericalWeights.put(numericalNames.get(i), numericalWeightsArr[i]);
			}
		}
		if(!categoricalEntropy.isEmpty()){
			double[] categoricalWeightsArr = generateWeighting(categoricalEntropy.toArray(new Double[0]));
			i = 0;
			int numCategorical = categoricalNames.size();
			for(; i < numCategorical; i++) {
				categoricalWeights.put(categoricalNames.get(i), categoricalWeightsArr[i]);
			}
		}
	}
	
	/**
	 * Generate the weighting values for a list of attributes based on their entropies
	 * @param entropyArr	double[] containing the entropy values for each attribute
	 * @return				double[] containing the weight value for each attribute
	 */
	public static double[] generateWeighting(final double[] entropyArr) {
		int size = entropyArr.length;
		
		double[] weight = new double[size];
		
		double totalEntropy = StatisticsUtilityMethods.getSum(entropyArr);
		int i = 0;
		for(; i < size; i++) {
			if(totalEntropy == 0) {
				weight[i] = 0;
			} else {
				weight[i] = entropyArr[i] / totalEntropy;
			}
		}
		
		return weight;
	}
	
	/**
	 * Generate the weighting values for a list of attributes based on their entropies
	 * @param entropyArr	double[] containing the entropy values for each attribute
	 * @return				double[] containing the weight value for each attribute
	 */
	public static double[] generateWeighting(final Double[] entropyArr) {
		int size = entropyArr.length;
		
		double[] weight = new double[size];
		
		double totalEntropy = StatisticsUtilityMethods.getSum(entropyArr);
		int i = 0;
		for(; i < size; i++) {
			if(totalEntropy == 0) {
				weight[i] = 0;
			} else {
				weight[i] = entropyArr[i] / totalEntropy;
			}
		}
		
		return weight;
	}
	
}
