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
import java.util.Hashtable;

import prerna.algorithm.learning.similarity.ClusterRemoveDuplicates;
import prerna.algorithm.learning.similarity.ClusteringDataProcessor;
import prerna.algorithm.learning.similarity.ClusteringNumericalMethods;
import prerna.algorithm.learning.unsupervised.ClusterUtilityMethods;
import prerna.algorithm.learning.unsupervised.ClusteringOptimization;
import prerna.algorithm.learning.unsupervised.PartitionedClusteringAlgorithm;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayListUtilityMethods;
import prerna.util.ArrayUtilityMethods;

/**
 * Algorithm logic:
 * 1) Separate instances into separate clusters based on one specific property(column)
 * 2) Calculate the centers for each cluster (excluding the column used to separate them originally)
 * 3) Determine for each instance, which cluster it is most similar to based on all properties (except the one used to separate them originally)
 * 4) Keep track of where the instance is closest to and if it is the same cluster as when it was originally separated
 * 
 */
public class ClusteringClassification {

	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	private int[][] originalClusterAssignedIndices;
	private int[][] newClusterAssignedIndices;
	private int[][][] countMatrix;
	
	private double[] accuracy;
	private double[] precision;

	public double[] getAccuracy() {
		return accuracy.clone();
	}

	public double[] getPrecision() {
		return precision.clone();
	}

	public ClusteringClassification(ArrayList<Object[]> masterTable, String[] masterNames) {
		ClusterRemoveDuplicates formatter = new ClusterRemoveDuplicates(masterTable, masterNames);
		this.masterTable = formatter.getRetMasterTable();
		this.masterNames = formatter.getRetVarNames();
	}
	
	public void execute() {
		int i;
		int numCols = masterTable.get(0).length;
		int numRows = masterTable.size();
		
		countMatrix = new int[numCols - 1][][];
		originalClusterAssignedIndices = new int[numCols][numRows];
		newClusterAssignedIndices = new int[numCols][numRows];
		
		for(i = 1; i < numCols; i++) {
			ArrayList<Object[]> clusterData = useColumnFromList(masterTable, i);
			String[] clusterNames = useNameFromList(masterNames, i);
			
			ClusteringOptimization alg = new ClusteringOptimization(clusterData, clusterNames);
			alg.setDataVariables();
			int minClusters = 2;
			int maxClusters = 50;
			((PartitionedClusteringAlgorithm) alg).generateBaseClusterInformation(50);
			((ClusteringOptimization) alg).runGoldenSelectionForNumberOfClusters(minClusters, maxClusters);
			((PartitionedClusteringAlgorithm) alg).generateInitialClusters();
			alg.execute();
				
			originalClusterAssignedIndices[i] = alg.getClusterAssignment();
			int[] numInstancesInCluster = alg.getNumInstancesInCluster();
			
			ArrayList<Object[]> data = ArrayListUtilityMethods.removeColumnFromList(masterTable, i);
			String[] names = ArrayUtilityMethods.removeNameFromList(masterNames, i);
			// process through data
			ClusteringDataProcessor cdp = new ClusteringDataProcessor(data, names);
			// take results of data processing to generate cluster centers
			ClusteringNumericalMethods cnm = new ClusteringNumericalMethods(cdp.getNumericalBinMatrix(), cdp.getCategoricalMatrix(), cdp.getNumericalBinOrderingMatrix());
			cnm.setCategoricalWeights(cdp.getCategoricalWeights());
			cnm.setNumericalWeights(cdp.getNumericalWeights());
			
			// generate cluster centers
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix = cnm.generateCategoricalClusterCenter(originalClusterAssignedIndices[i]);
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix = cnm.generateNumericClusterCenter(originalClusterAssignedIndices[i]);
			
			// determine where each instance is closest to but never change the state
			newClusterAssignedIndices[i] = new int[originalClusterAssignedIndices[i].length];
			int j;
			for(j = 0; j < numRows; j++) {
				int instanceInd = originalClusterAssignedIndices[i][j];
				int mostSimilarCluster = ClusterUtilityMethods.findNewClusterForInstance(cnm, clusterCategoryMatrix, clusterNumberBinMatrix, numInstancesInCluster, instanceInd);
				newClusterAssignedIndices[i][instanceInd] = mostSimilarCluster;
			}
			calculateMatrix(originalClusterAssignedIndices[i], newClusterAssignedIndices[i], i-1);
		}
		
		double[] accuracy = calculateAccuracy();
		double[] precision = calculatePercision();
		
		for(i = 0; i < accuracy.length; i++) {
			System.out.println("Accuracy for " + masterNames[i+1] + ": "
					+ String.format("%.2f%%", accuracy[i])
					+ "\n---------------------------------");
			System.out.println("Percision for " + masterNames[i+1] + ": "
					+ String.format("%.2f", precision[i])
					+ "\n---------------------------------");
		}
	}
	
	
	private double[] calculateAccuracy() {
		accuracy = new double[countMatrix.length];
		int numRows = newClusterAssignedIndices[0].length;

		int i;
		int j;
		for(i = 0; i < countMatrix.length; i++) {
			int[][] innerMatrix = countMatrix[i];
			int sumCorrect = 0;
			for(j = 0; j < innerMatrix.length; j++) {
				sumCorrect += innerMatrix[j][j];
			}
			accuracy[i] = (double) sumCorrect/numRows;
		}
		
		return accuracy;
	}
	
	public double[] calculatePercision() {
		precision = new double[countMatrix.length];
		
		double[] pObs;
		if(accuracy == null) {
			pObs = calculateAccuracy();
		} else {
			pObs = accuracy;
		}
		
		double[] pExp = new double[countMatrix.length];
		int i;
		int j;
		int k;
		for(i = 0; i < countMatrix.length; i++) {
			int[][] innerMatrix = countMatrix[i];
			int size = innerMatrix.length;
			
			int[] sumRows = new int[size];
			int[] sumCols = new int[size];
			for(j = 0; j < size; j++) {
				for(k = 0; k < size; k++) {
					sumRows[j] += innerMatrix[k][j];
					sumCols[j] += innerMatrix[j][k];
				}
			}
			
			int total = 0;
			for(j = 0; j < size; j++) {
				total += sumRows[j];
			}
			
			for(j = 0; j < size; j++) {
				pExp[i] += (double) sumRows[j] * sumCols[j] / Math.pow(total,2);
			}
		}
		
		for(i = 0; i < countMatrix.length; i++) {
			precision[i] = ( pObs[i] - pExp[i] ) / (1 - pExp[i]);
		}
		
		return precision;
	}

	private void calculateMatrix(int[] oldClusterAssignedIndices,int[] newClusterAssignedIndices, int col) {
		int numClusters = StatisticsUtilityMethods.getMaximumValue(oldClusterAssignedIndices) + 1;

		int[][] innerMatrix = new int[numClusters][numClusters];
		
		int size = oldClusterAssignedIndices.length;

		int i;
		for(i = 0; i < size; i++) {
			innerMatrix[oldClusterAssignedIndices[i]][newClusterAssignedIndices[i]]++;
		}
		
		countMatrix[col] = innerMatrix;
	}

	private ArrayList<Object[]> useColumnFromList(ArrayList<Object[]> list, int colToUse) {
		int numRows = list.size();
		ArrayList<Object[]> retList = new ArrayList<Object[]>(numRows);
		
		int i;
		for(i = 0; i < numRows; i++) {
			Object[] newRow = new Object[2];
			Object[] oldRow = list.get(i);
			
			newRow[0] = oldRow[0];
			newRow[1] = oldRow[colToUse];
			
			retList.add(newRow);
		}
		
		return retList;
	}
	
	private String[] useNameFromList(String[] name, int colToUse) {
		String[] retNames = new String[2];
		retNames[0] = name[0];
		retNames[1] = name[colToUse];
		
		return retNames;
	}
}
