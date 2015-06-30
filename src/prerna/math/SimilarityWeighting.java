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

public final class SimilarityWeighting {

	private SimilarityWeighting() {
		
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
