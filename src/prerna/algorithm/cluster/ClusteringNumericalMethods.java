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
package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class ClusteringNumericalMethods extends AbstractNumericalMethods{

	private static final Logger LOGGER = LogManager.getLogger(ClusterUtilityMethods.class.getName());

	public ClusteringNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] instanceNumberBinOrderingMatrix) {
		super(numericalBinMatrix, categoricalMatrix, instanceNumberBinOrderingMatrix);
	}

	/**
	 * 
	 * @param numericalBinMatrix			All the numeric bin data for every instance
	 * @param categoricalMatrix				All the categorical bin data for every instance
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 */
	public Double getSimilarityScore(
			int dataIdx,
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo, 
			ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{		
		double numericalSimilarity = (double) 0;
		double categorySimilarity = (double) 0;

		if(numericalBinMatrix != null) {
			String[] instanceNumericalInfo = numericalBinMatrix[dataIdx];
			numericalSimilarity = calculateNumericalSimilarityUsingEntropy(numericPropNum, totalPropNum, numericalWeights, instanceNumericalInfo, numericalClusterInfo);
		}

		if(categoricalMatrix != null) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			categorySimilarity = calculateSimilarityUsingEntropy(categoricalPropNum, totalPropNum, categoricalWeights, instaceCategoricalInfo, categoryClusterInfo);
		}

		return numericalSimilarity + categorySimilarity;
	}

	/**
	 * Calculates the similarity score for the categorical/numerical-bin entries
	 * @param propNum					The number of categorical/numerical props
	 * @param totalProps				The total number of props used in clustering algorithm
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public double calculateSimilarityUsingEntropy(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			String[] instaceCategoricalInfo, 
			ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// sumProperties contains the total number of instances for the property
			int sumProperties = 0;
			Hashtable<String, Integer> propertyHash = categoryClusterInfo.get(i);
			Set<String> propKeySet = propertyHash.keySet();
			for(String propName : propKeySet) {
				sumProperties += propertyHash.get(propName);
			}
			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			int numOccuranceInCluster = 0;
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			similarity += weights[i] * (double) numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		//		LOGGER.info("Calculated similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

	/**
	 * Calculates the similarity score for the categorical/numerical-bin entries
	 * @param propNum					The number of categorical/numerical props
	 * @param totalProps				The total number of props used in clustering algorithm
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public double calculateNumericalSimilarityUsingEntropy(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			String[] instaceCategoricalInfo, 
			ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			if(weights[i] == 0) {
				continue;
			}

			String[] sortedBinArr = instanceNumberBinOrderingMatrix[i];
			// check to make sure instances contain value for prop and not all missing data
			if(sortedBinArr != null) {
				// numBins contains the number of bins
				int numBins = sortedBinArr.length;

				double adjustmentFactor = 0;
				Hashtable<String, Integer> propertyHash = categoryClusterInfo.get(i);
				Set<String> propKeySet = propertyHash.keySet();
				// sumProperties contains the total number of instances for the property
				int sumProperties = 0;
				for(String propName : propKeySet) {
					sumProperties += propertyHash.get(propName);
				}
				// deal with empty values
				if(instaceCategoricalInfo[i].equals("NaN")) {
					if(propKeySet.contains("NaN")) {
						double normalizedNumInBin = (double) propertyHash.get("NaN") / sumProperties;
						adjustmentFactor += normalizedNumInBin;
					} 
					//	else {
					//	// similarity is zero if one value is NaN and the other is not NaN
					//	}
				} else {
					for(String propName : propKeySet) {
						if(!propName.equals("NaN")) {
							double normalizedNumInBin = (double) propertyHash.get(propName) / sumProperties;
							int indexOfInstance = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, instaceCategoricalInfo[i]);
							int indexOfPropInCluster = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, propName);
							adjustmentFactor += normalizedNumInBin * calculateAdjustmentFactor(indexOfInstance, indexOfPropInCluster, numBins); 
						}
					}
				}
				similarity += weights[i] * adjustmentFactor;
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		// LOGGER.info("Calculated similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

	/**
	 * @param clusterIdx1
	 * @param clusterIdx2
	 * @param clusterNumericalMatrix
	 * @param clusterCategoryMatrix
	 * @return
	 */
	public double calculateClusterToClusterSimilarity(
			int clusterIdx1, 
			int clusterIdx2, 
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterNumericalMatrix, 
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix)
	{
		double numericSimilarity = 0;
		double categoricalSimilarity = 0;

		if(clusterNumericalMatrix != null) {
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo1 = clusterNumericalMatrix.get(clusterIdx1);
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo2 = clusterNumericalMatrix.get(clusterIdx2);

			numericSimilarity = calculateClusterNumericalSimilarity(numericPropNum, totalPropNum, numericalWeights, numericalClusterInfo1, numericalClusterInfo2);
		}

		if(clusterCategoryMatrix != null) {
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo1 = clusterCategoryMatrix.get(clusterIdx1);
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo2 = clusterCategoryMatrix.get(clusterIdx2);

			categoricalSimilarity = calculateClusterSimilarity(categoricalPropNum, totalPropNum, categoricalWeights, categoricalClusterInfo1, categoricalClusterInfo2);
		}

		return numericSimilarity + categoricalSimilarity;
	}

	/**
	 * 
	 * @param propNum
	 * @param totalPropNum
	 * @param weights
	 * @param clusterInfo1
	 * @param clusterInfo2
	 * @return 
	 */
	private double calculateClusterSimilarity(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			ArrayList<Hashtable<String, Integer>> clusterInfo1, 
			ArrayList<Hashtable<String, Integer>> clusterInfo2) 
	{
		double coeff = 1.0 * propNum / totalPropNum;
		double similarityScore = 0;

		int i;
		int size = clusterInfo1.size();
		// loop through all properties
		for(i = 0; i < size; i++) {
			// for specific property
			Hashtable<String, Integer> propInfoForCluster1 = clusterInfo1.get(i);
			Hashtable<String, Integer> propInfoForCluster2 = clusterInfo2.get(i);

			int normalizationCount1 = 0;
			for(String propInstance : propInfoForCluster1.keySet()) {
				normalizationCount1 += propInfoForCluster1.get(propInstance);
			}
			int normalizationCount2 = 0;
			for(String propInstance : propInfoForCluster2.keySet()) {
				normalizationCount2 += propInfoForCluster2.get(propInstance);
			}

			int possibleValues = 0;
			double sumClusterDiff = 0;
			for(String propInstance : propInfoForCluster1.keySet()) {
				if(propInfoForCluster2.containsKey(propInstance)) {
					possibleValues++;
					// calculate difference between counts
					int count1 = propInfoForCluster1.get(propInstance);
					int count2 = propInfoForCluster2.get(propInstance);
					sumClusterDiff += Math.abs((double) count1/normalizationCount1 - (double) count2/normalizationCount2);
				} else {
					possibleValues++;
					//include values that 1st cluster has and 2nd cluster doesn't have
					int count1 = propInfoForCluster1.get(propInstance);
					sumClusterDiff += (double) count1/normalizationCount1;
				}
			}
			//now include values that 2nd cluster has that 1st cluster doesn't have
			for(String propInstance: propInfoForCluster2.keySet()) {
				if(!propInfoForCluster1.containsKey(propInstance)) {
					possibleValues++;
					int count2 = propInfoForCluster2.get(propInstance);
					sumClusterDiff += (double) count2/normalizationCount2;
				}
			}

			similarityScore += weights[i] * (1 - sumClusterDiff/possibleValues);
		}

		return coeff * similarityScore;
	}


	/**
	 * 
	 * @param propNum
	 * @param totalPropNum
	 * @param weights
	 * @param clusterInfo1
	 * @param clusterInfo2
	 * @return 
	 */
	private double calculateClusterNumericalSimilarity(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			ArrayList<Hashtable<String, Integer>> clusterInfo1, 
			ArrayList<Hashtable<String, Integer>> clusterInfo2) 
	{
		double coeff = 1.0 * propNum / totalPropNum;
		double similarityScore = 0;

		int i;
		int size = clusterInfo1.size();
		// loop through all properties
		for(i = 0; i < size; i++) {
			// for specific property
			Hashtable<String, Integer> propInfoForCluster1 = clusterInfo1.get(i);
			Hashtable<String, Integer> propInfoForCluster2 = clusterInfo2.get(i);

			String[] sortedBinArr = instanceNumberBinOrderingMatrix[i];
			// numBins contains the number of bins
			if(sortedBinArr != null) {
				int numBins = sortedBinArr.length;
				Double[] numInstances1 = calculateComparisonArr(propInfoForCluster1, sortedBinArr);
				Double[] numInstances2 = calculateComparisonArr(propInfoForCluster2, sortedBinArr);
	
				double normalizationCount1 = StatisticsUtilityMethods.getSum(numInstances1);
				double normalizationCount2 = StatisticsUtilityMethods.getSum(numInstances2);
	
				double sumClusterDiff = 0;
				for(int index = 0; index < numBins+1; index++) {
					double count1 = numInstances1[index];
					double count2 = numInstances2[index];
					sumClusterDiff += Math.abs(count1/normalizationCount1 - count2/normalizationCount2);
				}
	
				similarityScore += weights[i] * (1 - sumClusterDiff/numBins);
			}
		}

		return coeff * similarityScore;
	}

	private Double[] calculateComparisonArr(Hashtable<String, Integer> propInfoForCluster, String[] sortedBinArr){
		int numBins = sortedBinArr.length;
		Double[] numInstances = new Double[numBins]; 
		Double[] adjustedArr = new Double[numBins];
		int j;
		// Set NaN to be the first bin when creating comparison
		for(String propInstance : propInfoForCluster.keySet()) {
			if(!propInstance.equals("NaN")) {
				int index = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, propInstance);
				int count = propInfoForCluster.get(propInstance);
				numInstances[index] = (double) count;
			}
		}
		for(j = 0; j < numBins; j++) {
			if(numInstances[j] == null) {
				//if we don't have any instances with this specific bin value, find closest non-null value and add adjustment factor
				int[] indicies;
				try {
					indicies = ArrayUtilityMethods.findAllClosestNonNullIndex(numInstances, j+1);
				} catch(NullPointerException ex) {
					// this means all information in cluster is NaN
					break;
				}
				double innerCount = 0;
				for(int k = 0; k < indicies.length; k++) {
					innerCount += numInstances[indicies[k]];
				}
				innerCount /= indicies.length;
				adjustedArr[j] = innerCount * calculateAdjustmentFactor(indicies[0], j, numBins); 
			} else {
				adjustedArr[j] = numInstances[j];
			}
		}

		Double[] retArr = new Double[numBins+1];
		System.arraycopy(adjustedArr, 0, retArr, 1, numBins);
		if(propInfoForCluster.keySet().contains("NaN")) {
			retArr[0] = (double) propInfoForCluster.get("NaN");
		} else {
			retArr[0] = 0.0;
		}
		for(j = 0; j < numBins+1; j++) {
			if(retArr[j] == null) {
				retArr[j] = 0.0;
			}
		}

		return retArr;
	}

	public ArrayList<ArrayList<Hashtable<String, Integer>>> generateNumericClusterCenter(int[] clusterAssigned) {
		int numClusters = StatisticsUtilityMethods.getMaximumValue(clusterAssigned) + 1;

		if(numericalBinMatrix != null) {
			ArrayList<ArrayList<Hashtable<String, Integer>>> numericClusterCenter = ClusterUtilityMethods.createClustersCategoryProperties(numericalBinMatrix, clusterAssigned, numClusters);
			return numericClusterCenter;
		}

		return null;
	}

	public ArrayList<ArrayList<Hashtable<String, Integer>>> generateCategoricalClusterCenter(int[] clusterAssigned) {
		int numClusters = StatisticsUtilityMethods.getMaximumValue(clusterAssigned) + 1;

		if(categoricalMatrix != null) {
			ArrayList<ArrayList<Hashtable<String, Integer>>> categoricalClusterCenter = ClusterUtilityMethods.createClustersCategoryProperties(categoricalMatrix, clusterAssigned, numClusters);
			return categoricalClusterCenter;
		}

		return null;
	}
	
}
