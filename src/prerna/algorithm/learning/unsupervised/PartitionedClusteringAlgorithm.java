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
package prerna.algorithm.learning.unsupervised;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.similarity.ClusteringNumericalMethods;
import prerna.util.ArrayUtilityMethods;

public class PartitionedClusteringAlgorithm extends ClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(PartitionedClusteringAlgorithm.class.getName());

	private int[] masterOrderedOriginalClusterAssingment;

	protected ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterCategoricalMatrix;
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterNumberBinMatrix;

	private int partitionSize;
	private int straglers;
	private double[] numericalEntropyArr;
	private double[] categoricalEntropyArr;

	private Queue<Integer> processingQueue;
	private int numProcessors;
	
	public PartitionedClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}

	public void generateBaseClusterInformation(int maximumNumberOfClusters) {
		numericalEntropyArr = cdp.getNumericalEntropy();
		categoricalEntropyArr = cdp.getCategoricalEntropy();
		
		cnm = new ClusteringNumericalMethods(instanceNumberBinMatrix, instanceCategoryMatrix, instanceNumberBinOrderingMatrix);
		cnm.setCategoricalWeights(categoricalWeights);
		cnm.setNumericalWeights(numericalWeights);

		randomlyAssignClusters(numInstances, maximumNumberOfClusters); // creates clusterAssignment and orderedOriginalClusterAssignment
		masterOrderedOriginalClusterAssingment = orderedOriginalClusterAssignment.clone();

		numProcessors = Runtime.getRuntime().availableProcessors();
	}

	public void generateInitialClusters() {
		LOGGER.info("Generating Initial Clustes ");
		numInstancesInCluster = initalizeClusterMatrix(numClusters);

		instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		instanceCategoryMatrix = cdp.getCategoricalMatrix();

		randomlyAssignClustersFromValues(numInstances, numClusters);  // creates clusterAssignment and orderedOriginalClusterAssignment
		//make the custer number matrix from initial assignments
		initialClusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clusterAssignment, numClusters);
		//make the cluster category matrix from initial assignments
		initialClusterCategoricalMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clusterAssignment, numClusters);

		clusterCategoryMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>(numClusters);
		clusterNumberBinMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>(numClusters);
		copyHash(initialClusterCategoricalMatrix, clusterCategoryMatrix);
		copyHash(initialClusterNumberBinMatrix, clusterNumberBinMatrix);
	}

	public void randomlyAssignClustersFromValues(int numInstances, int numClusters) {
		clusterAssignment = new int[numInstances];
		orderedOriginalClusterAssignment = new int[numClusters];
		int i = 0;
		for(; i < numInstances; i++) {
			clusterAssignment[i] = -1;
		}

		i = 0;
		for(; i < numClusters; i++) {
			int position = masterOrderedOriginalClusterAssingment[i];
			clusterAssignment[position] = i;
			orderedOriginalClusterAssignment[i] = position;
		}
		
		int numRows = 0;
		int numCols = 0;
		if(instanceNumberBinMatrix != null) {
			numRows = instanceNumberBinMatrix.length;
			numCols += instanceNumberBinMatrix[0].length;
		}
		if(instanceCategoryMatrix != null) {
			numRows = instanceCategoryMatrix.length;
			numCols += instanceCategoryMatrix[0].length;
		}
		calculatePartitionSize(numRows, numCols);
	}

	@Override
	public boolean execute() {
		success = true;

		double[] categoricalWeights = cdp.getCategoricalWeights();
		double[] numericalWeights = cdp.getNumericalWeights();
		numInstancesInCluster = initalizeClusterMatrix(numClusters);

		processingQueue = new PriorityQueue<Integer>();
		ArrayList<Thread> threads = new ArrayList<Thread>();
		int i = 0;
		synchronized(processingQueue) {
			for(; i < numInstances; i += partitionSize) {
				LOGGER.info("Staring data processing for " + i);

				AbstractClusteringAlgorithm partitionedAlg = new ClusteringAlgorithm();
				
				((ClusteringAlgorithm) partitionedAlg).setTotalClusterAssignment(clusterAssignment);
				((ClusteringAlgorithm) partitionedAlg).setTotalNumInstancesInCluster(numInstancesInCluster);
				((ClusteringAlgorithm) partitionedAlg).setTotalClusterCategoricalMatrix(clusterCategoryMatrix);
				((ClusteringAlgorithm) partitionedAlg).setTotalClusterNumberBinMatrix(clusterNumberBinMatrix);
				
				int partitionNumInstances = partitionSize;
				int val = i + partitionSize;
				// determine if last portion will not split evenly and add to last partition
				if(val != numInstances && val + partitionSize > numInstances && (val + partitionSize)%numInstances != 0) {
					val = numInstances;
					partitionNumInstances = numInstances - i;
				}
				
				partitionedAlg.setVarNames(varNames);
				partitionedAlg.setCategoricalWeights(categoricalWeights);
				partitionedAlg.setNumericalWeights(numericalWeights);
				partitionedAlg.setCategoryPropNames(categoryPropNames);
				partitionedAlg.setNumericalPropNames(numericalPropNames);
				partitionedAlg.setNumericalPropIndices(numericalPropIndices);
				partitionedAlg.setCategoricalPropIndices(categoryPropIndices);
				partitionedAlg.setNumInstances(partitionNumInstances);
				partitionedAlg.setNumClusters(numClusters);
				partitionedAlg.setRecreateStartingClusterValues(false);
				((ClusteringAlgorithm) partitionedAlg).setRemoveEmptyClusters(false);
				((ClusteringAlgorithm) partitionedAlg).setParitionIndex(i);
				((ClusteringAlgorithm) partitionedAlg).setProcessingQueue(processingQueue);
				
				String[][] partitonedNumberBinMatrix = null;
				String[][] partitonedCategoryMatrix = null;
				if(instanceNumberBinMatrix != null) {
					partitonedNumberBinMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(instanceNumberBinMatrix, i, val);
				}
				if(instanceCategoryMatrix != null) {
					partitonedCategoryMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(instanceCategoryMatrix, i, val);
				}

				int[] partitonedClustersAssignment = Arrays.copyOfRange(clusterAssignment, i, val);
				partitionedAlg.setClusterAssignment(partitonedClustersAssignment);
				partitionedAlg.setInstanceCategoricalMatrix(partitonedCategoryMatrix);
				partitionedAlg.setInstanceNumberBinMatrix(partitonedNumberBinMatrix);
				partitionedAlg.setInstanceNumberBinOrderingMatrix(instanceNumberBinOrderingMatrix);

				// need to create new objects so original bin matrix does not get changes
				ArrayList<ArrayList<Hashtable<String, Integer>>> partitionedInitialClusterCategoricalMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>();
				copyHash(initialClusterCategoricalMatrix, partitionedInitialClusterCategoricalMatrix);
				ArrayList<ArrayList<Hashtable<String, Integer>>> partitionedClusterNumberBinMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>();
				copyHash(initialClusterNumberBinMatrix, partitionedClusterNumberBinMatrix);

				partitionedAlg.setClusterCategoricalMatrix(partitionedInitialClusterCategoricalMatrix);
				partitionedAlg.setClusterNumberBinMatrix(partitionedClusterNumberBinMatrix);

				processingQueue.add(i);
				while(processingQueue.size() >= numProcessors * 2) {
					try {
						LOGGER.info("Waiting for queue...");
						processingQueue.wait();
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
				Thread t = new Thread((ClusteringAlgorithm) partitionedAlg);
				threads.add(t);
				t.start();
				// TODO: how to determine if error occured in separate thread?
			}
		}
		for(Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		createClusterSummaryRowsForGrid();

		return success;
	}

	private void calculatePartitionSize(int numRows, int numCols) {
		int numPartitions = (int) Math.ceil((double) numRows*numCols/40_000);

		boolean lowEntropy = false;
		int i = 0;
		if(categoricalEntropyArr != null) {
			int size = categoricalEntropyArr.length;
			for(; i < size; i++) {
				if(categoricalEntropyArr[i] < 0.2) {
					lowEntropy = true;
					numPartitions = (int) Math.ceil((double) numRows*numCols/5_700);
					break;
				}
			}
		}
		if(!lowEntropy) {
			i = 0;
			if(numericalEntropyArr != null) {
				int size = numericalEntropyArr.length;
				for(; i < size; i++) {
					if(numericalEntropyArr[i] < 0.2) {
						lowEntropy = true;
						numPartitions = (int) Math.ceil((double) numRows*numCols/5_700);
						break;
					}
				}
			}
		}
		partitionSize = (int) Math.ceil((double) numRows/numPartitions);
	}

	private void copyHash(final ArrayList<ArrayList<Hashtable<String, Integer>>> original, ArrayList<ArrayList<Hashtable<String, Integer>>> copyToHash) {
		if(original != null) {
			for(ArrayList<Hashtable<String, Integer>> innerList : original) {
				ArrayList<Hashtable<String, Integer>> copyList = new ArrayList<Hashtable<String, Integer>>();
				for(Hashtable<String, Integer> hash : innerList) {
					Hashtable<String, Integer> copyHash = new Hashtable<String, Integer>();
					for(String prop : hash.keySet()) {
						int val = hash.get(prop);
						copyHash.put(prop, val);
					}
					copyList.add(copyHash);
				}
				copyToHash.add(copyList);
			}
		}
	}
}
