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

import java.util.Queue;

import prerna.util.ArrayUtilityMethods;

public class InstanceNumericalMethods extends AbstractNumericalMethods implements Runnable {

	// used for running multiple threads
	private int index; 
	private int start;
	private int numInstances;
	private Queue<Integer> processingQueue;
	private boolean calculateSimMatrix = true;
	private boolean calculateKSimMatrix = false;
	private boolean calculateReachability = false;
	private boolean calculateLRD = false;

	private int k;
	private double[][] similarityMatrix;
	double[] kSimilarityArr;
	int[][] kSimilarityIndicesMatrix;
	private double[][] reachSimMatrix;
	private double[] lrd;

	public InstanceNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] numericalBinOrderingMatrix) {
		super(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
	}

	@Override
	public void run() {
		// loop through and calculate the similarity between index and all other indices and update the similarity matrix
		if(calculateSimMatrix) {
			int i = start;
			int size = numInstances-start;
			double[] values = new double[size];
			int counter = 0;
			for(;i < numInstances; i++) {
				values[counter] = calculateSimilarityBetweenInstances(index, i);
				counter++;
			}
			i = 0;
			synchronized(processingQueue){
				for(; i < size; i++) {
					double val = values[i];
					similarityMatrix[index][start+i] = val;
					similarityMatrix[start+i][index] = val;
				}
				processingQueue.remove(index);
				processingQueue.notify();
			}
		} else if(calculateKSimMatrix) {
			double[] similarityArrBetweenInstanceToAllOtherInstances = similarityMatrix[index];
			int[] kNearestNeighbors = kNearestNeighbors(similarityArrBetweenInstanceToAllOtherInstances.clone(), index, k);
			// kNearestNeighbors has the most similar similar index first and least similar index last
			synchronized(processingQueue) {
				if(kNearestNeighbors != null && kNearestNeighbors.length != 0) {
					int mostDistantNeighborIndex = kNearestNeighbors[kNearestNeighbors.length - 1];
					kSimilarityArr[index] = similarityArrBetweenInstanceToAllOtherInstances[mostDistantNeighborIndex];
					kSimilarityIndicesMatrix[index] = kNearestNeighbors;
				} else {
					kSimilarityArr[index] = 0;
				}
				processingQueue.remove(index);
				processingQueue.notify();
			}
		} else if(calculateReachability) {
			int i = 0;
			int size = numInstances;
			double[] values = new double[size];
			int counter = 0;
			for(; i < numInstances; i++) {
				// reach similarity is the minimum of the similarity between object i and j and the min similarity of the k closest clusters
				if(kSimilarityArr[i] == 0 || kSimilarityArr[index] == 0) {
					values[counter] = 0;
				} else {
					values[counter] = Math.min(similarityMatrix[i][index], kSimilarityArr[i]);
				}
				counter++;
			}
			synchronized(processingQueue) {
				reachSimMatrix[index] = values;
				processingQueue.remove(index);
				processingQueue.notify();
			}
		}
//		else if(calculateLRD) {
//			int[] kClosestNeighbors = kSimilarityIndicesMatrix[index];
//			double val = 0;
//			double sumReachSim = 0;
//			if(kClosestNeighbors != null) {
//				for(int j : kClosestNeighbors) {
//					sumReachSim += reachSimMatrix[index][j];
//				}
//				val = sumReachSim/kClosestNeighbors.length;
//			} else {
//				val = 0;
//			}
//			synchronized(processingQueue) {
//				lrd[index] = val;
//				processingQueue.remove(index);
//				processingQueue.notify();
//			}
//		} 
		else {
			throw new IllegalArgumentException("Must either be calculating the similarity matrix or the k-similarity matrix for the given index");
		}
	}

	public double calculateSimilarityBetweenInstances(int instanceIdx1, int instanceIdx2) {
		double categoricalSimilarityScore = 0;
		double numericalSimilarityScore = 0;

		if(categoricalMatrix != null) {
			String[] categoricalValuesForInstance1 = categoricalMatrix[instanceIdx1];
			String[] categoricalValuesForInstance2 = categoricalMatrix[instanceIdx2];

			categoricalSimilarityScore = calculateInstanceSimilarity(categoricalValuesForInstance1, categoricalValuesForInstance2, categoricalWeights, categoricalPropNum, totalPropNum);
		}

		if(numericalBinMatrix != null) {
			String[] numericalBinValuesForInstance1 = numericalBinMatrix[instanceIdx1];
			String[] numericalBinValuesForInstance2 = numericalBinMatrix[instanceIdx2];

			numericalSimilarityScore = calculateNumericalInstanceSimilarity(numericalBinValuesForInstance1, numericalBinValuesForInstance2, numericalWeights, numericPropNum, totalPropNum);
		}	

		return categoricalSimilarityScore + numericalSimilarityScore;
	}

	public double calculateInstanceSimilarity(String[] categoricalValues1, String[] categoricalValues2, double[] weights, int propNum, int totalPropNum){
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// the values are either the same or different, which results in either adding the weight*1/1 = weight or weight*0/2 = 0
			if(categoricalValues1[i].equals(categoricalValues2[i])) {
				similarity += weights[i];
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		//		LOGGER.info("Calculated instance similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

	public double calculateNumericalInstanceSimilarity(String[] categoricalValues1, String[] categoricalValues2, double[] weights, int propNum, int totalPropNum){
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// deal with empty values
			if(categoricalValues1[i].equals("NaN")) {
				if(categoricalValues2[i].equals("NaN")) {
					similarity += weights[i];
				} 
				//				else {
				//					// similarity is zero if one value is NaN and the other is not NaN
				//				}
			} else {
				// the values are either the same or different, which results in either adding the weight*1/1 = weight or weight*0/2 = 0
				String[] sortedBinArr = instanceNumberBinOrderingMatrix[i];
				// numBins contains the number of bins
				int numBins = sortedBinArr.length;
				int indexOfInstance1 = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, categoricalValues1[i]);
				int indexOfInstance2 = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, categoricalValues2[i]);
				// adjustment factor simplifies since comparing only one instance to another instance
				double adjustmentFactor = calculateAdjustmentFactor(indexOfInstance1, indexOfInstance2, numBins); 
				similarity += weights[i] * adjustmentFactor;
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		//		LOGGER.info("Calculated instance similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

	public int[] kNearestNeighbors(double[] similarityBetweenInstanceToAllOtherInstances, int instanceIdx, int k) {
		int[] kClosestNeighbors = new int[k];
		int numInstances = similarityBetweenInstanceToAllOtherInstances.length;
		int[] indexOrderArr = indexOrder(numInstances);

		// loop through and order all values and indices
		int i;
		double tempSimValue;
		int tempIndexValue;
		boolean flag = true;
		while(flag) {
			flag = false;
			for(i = 0; i < numInstances - 1; i++) {
				if(similarityBetweenInstanceToAllOtherInstances[i] < similarityBetweenInstanceToAllOtherInstances[i+1]){
					tempSimValue = similarityBetweenInstanceToAllOtherInstances[i];
					similarityBetweenInstanceToAllOtherInstances[i] = similarityBetweenInstanceToAllOtherInstances[i+1];
					similarityBetweenInstanceToAllOtherInstances[i+1] = tempSimValue;
					// change order of index value
					tempIndexValue = indexOrderArr[i];
					indexOrderArr[i] = indexOrderArr[i+1];
					indexOrderArr[i+1] = tempIndexValue;

					flag = true;
				}
			}
		}

		int idxCounter = 0;
		int kCounter = 0;
		boolean noKCluster = false;
		while(kCounter < k && idxCounter < numInstances) {
			// don't include the node
			int index = indexOrderArr[idxCounter];
			if(index != instanceIdx) {	
				// don't include nodes with zero similarity
				if(similarityBetweenInstanceToAllOtherInstances[idxCounter] == 0) {
					noKCluster = true;
					return null;
				}
				kClosestNeighbors[kCounter] = index;
				kCounter++;
			}
			idxCounter++;
		}

		if(!noKCluster) {
			double lastSimValueInNeighborhood = similarityBetweenInstanceToAllOtherInstances[kCounter - 1];
			for(;idxCounter < numInstances; idxCounter++) {
				int index = indexOrderArr[idxCounter];
				if(index != instanceIdx) {
					double nextSimValueClosestToInstance = similarityBetweenInstanceToAllOtherInstances[idxCounter];
					if(lastSimValueInNeighborhood == nextSimValueClosestToInstance) {
						int newInstanceInNeighborhood =  indexOrderArr[idxCounter];
						try {
							kClosestNeighbors[kCounter] = newInstanceInNeighborhood;
						} catch (IndexOutOfBoundsException e) {
							kClosestNeighbors = ArrayUtilityMethods.resizeArray(kClosestNeighbors, 2);
							kClosestNeighbors[kCounter] = newInstanceInNeighborhood;
						}
						kCounter++;
					} else {
						break;
					}
				}
			}

			// to account for case when last index is 0, don't want to remove it from the K-Neighborhood
			if(kClosestNeighbors[kCounter - 1] == 0) {
				kClosestNeighbors[kCounter - 1] = 1;
				kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
				kClosestNeighbors[kCounter - 1] = 0;
			} else {
				kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
			}
		} else {
			kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
		}

		return kClosestNeighbors;
	}

	private int[] indexOrder(int length) {
		int[] retArr = new int[length];
		int i;
		for(i = 0; i < length; i++) {
			retArr[i] = i;
		}

		return retArr;
	}

	public boolean isCalculateSimMatrix() {
		return calculateSimMatrix;
	}

	public void setCalculateSimMatrix(boolean calculateSimMatrix) {
		this.calculateSimMatrix = calculateSimMatrix;
	}

	public boolean isCalculateReachability() {
		return calculateReachability;
	}

	public void setCalculateReachability(boolean calculateReachability) {
		this.calculateReachability = calculateReachability;
	}

	public boolean isCalculateKSimMatrix() {
		return calculateKSimMatrix;
	}

	public void setCalculateKSimMatrix(boolean calculateKSimMatrix) {
		this.calculateKSimMatrix = calculateKSimMatrix;
	}

	public boolean isCalculateLRD() {
		return calculateLRD;
	}

	public void setCalculateLRD(boolean calculateLRD) {
		this.calculateLRD = calculateLRD;
	}

	public void setProcessingQueue(Queue<Integer> processingQueue) {
		this.processingQueue = processingQueue;
	}

	public Queue<Integer> getProcessingQueue() {
		return processingQueue;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getStart() {
		return start;
	}

	public int getNumInstances() {
		return numInstances;
	}

	public void setNumInstances(int numInstances) {
		this.numInstances = numInstances;
	}

	public double[][] getSimilarityMatrix() {
		return similarityMatrix;
	}

	public void setSimilarityMatrix(double[][] similarityMatrix) {
		this.similarityMatrix = similarityMatrix;
	}

	public double[] getkSimilarityArr() {
		return kSimilarityArr;
	}

	public void setkSimilarityArr(double[] kSimilarityArr) {
		this.kSimilarityArr = kSimilarityArr;
	}

	public int[][] getkSimilarityIndicesMatrix() {
		return kSimilarityIndicesMatrix;
	}

	public void setkSimilarityIndicesMatrix(int[][] kSimilarityIndicesMatrix) {
		this.kSimilarityIndicesMatrix = kSimilarityIndicesMatrix;
	}

	public double[][] getReachSimMatrix() {
		return reachSimMatrix;
	}

	public void setReachSimMatrix(double[][] reachSimMatrix) {
		this.reachSimMatrix = reachSimMatrix;
	}

	public double[] getLrd() {
		return lrd;
	}

	public void setLrd(double[] lrd) {
		this.lrd = lrd;
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}
}
