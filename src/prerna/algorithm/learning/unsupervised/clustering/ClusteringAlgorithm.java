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
package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

/** Generic clustering algorithm to cluster instances based on their categorical and numerical properties.
 * 
 */
public class ClusteringAlgorithm extends AbstractClusteringAlgorithm implements Runnable{

	private static final Logger LOGGER = LogManager.getLogger(ClusteringAlgorithm.class.getName());
	private boolean removeEmptyClusters = true; 
	
	private Queue<Integer> processingQueue;
	private int[] totalNumInstancesInCluster;
	private int[] totalClusterAssignment;
	private ArrayList<ArrayList<Hashtable<String, Integer>>> totalClusterCategoricalMatrix;
	private ArrayList<ArrayList<Hashtable<String, Integer>>> totalClusterNumberBinMatrix;
	
	protected int partitionIndex;
	public void setParitionIndex(int partitionIndex) {
		this.partitionIndex = partitionIndex;
	}
	public int getPartitionIndex() {
		return partitionIndex;
	}
	
	public ClusteringAlgorithm() {
		
	}
	
	public ClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}
	
	@Override
	public void run() {
		execute();
		synchronized(processingQueue) {
			//update clustering results
			combineResults();
			
			numInstancesInCluster = updateArray(numInstancesInCluster, totalNumInstancesInCluster);
			System.arraycopy(clusterAssignment, 0, totalClusterAssignment, partitionIndex, numInstances);
			
			LOGGER.info("Notifying...");
			processingQueue.remove(partitionIndex);
			processingQueue.notify();
		}
	}
	
	/** Performs the clustering based off of the instance's categorical and numerical properties.
	 * These properties are pulled from the instanceCategoryMatrix and instanceNumberMatrix, that are filled prior to start.
	 * The final cluster each instance is assigned to is stored in clustersAssigned.
	 * The categorical and numerical properties for each cluster based on the instances it contains are stored in clusterCategoryMatrix and clusterNumberMatrix.
	 * The number of instances in each cluster is stored in clustersNumInstances.
	 */
	@Override
	public boolean execute() throws IllegalArgumentException {
		setAlgorithmVariables();
		
		boolean noChange = false;
		int iterationCount = 0;
		int maxIterations = 1000000;
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		while(!noChange && iterationCount <= maxIterations) {
			noChange = true;
			int instanceInd = 0;
			for(instanceInd = 0; instanceInd < numInstances; instanceInd++) {
				int newClusterForInstance = ClusterUtilityMethods.findNewClusterForInstance(cnm, clusterCategoryMatrix, clusterNumberBinMatrix, numInstancesInCluster, instanceInd);
				int oldClusterForInstance = clusterAssignment[instanceInd];
				if(newClusterForInstance != oldClusterForInstance) {
					noChange = false;
					clusterNumberBinMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceNumberBinMatrix, clusterNumberBinMatrix);
					clusterCategoryMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceCategoryMatrix, clusterCategoryMatrix);
					if(oldClusterForInstance > -1) {
						numInstancesInCluster[oldClusterForInstance]--;
					}
					numInstancesInCluster[newClusterForInstance]++;
					clusterAssignment[instanceInd] = newClusterForInstance;
				}
			}
			iterationCount++;
		}
		//if it quits after the ridiculously large number of times, print out the error
		if(iterationCount == maxIterations) {
			LOGGER.info("Completed Maximum Number of iterations without finding a solution");
			success = false;
		}
		else {
			success = true;
			//loop through and remove any empty clusters
			if(removeEmptyClusters) {
				int i;
				int nonEmptyClusterCount = numInstancesInCluster.length;
				int counter = 0;
				for(i = 0; i < nonEmptyClusterCount; i++) {
					if(numInstancesInCluster[i] == 0) {
						if(clusterNumberBinMatrix != null) {
							clusterNumberBinMatrix.remove(i - counter);
						}
						if(clusterCategoryMatrix != null) {
							clusterCategoryMatrix.remove(i - counter);
						}
						int j;
						int size = clusterAssignment.length;
						for(j = 0; j < size; j++) {
							if(clusterAssignment[j] > i - counter) {
								clusterAssignment[j]--;
							}
						}
						counter++;
					}
				}
				numInstancesInCluster = ArrayUtilityMethods.removeAllZeroValues(numInstancesInCluster);
				numClusters = numInstancesInCluster.length;
			}
		}
		createClusterSummaryRowsForGrid();
		return success;
	}
	
	private synchronized int[] updateArray(int[] original, int[] copyToArr) {
		int i = 0;
		int size = original.length;
		for(; i < size; i++) {
			copyToArr[i] += original[i];
		}

		return copyToArr;
	}

	private synchronized void combineResults() {
		// update categorical centers
		int i = 0;
		if(clusterCategoryMatrix != null) {
			int size = clusterCategoryMatrix.size();
			for(; i < size; i++) {
				ArrayList<Hashtable<String, Integer>> newClusterInfo = clusterCategoryMatrix.get(i);
				ArrayList<Hashtable<String, Integer>> currClusterInfo = totalClusterCategoricalMatrix.get(i);

				int j = 0;
				int numProps = newClusterInfo.size();
				for(; j < numProps; j++) {
					Hashtable<String, Integer> newPropInfo = newClusterInfo.get(j);
					Hashtable<String, Integer> currPropInfo = currClusterInfo.get(j);

					for(String prop : newPropInfo.keySet()) {
						int count = newPropInfo.get(prop);
						if(currPropInfo.containsKey(prop)) {
							count += currPropInfo.get(prop);
							currPropInfo.put(prop, count);
						} else {
							currPropInfo.put(prop, count);
						}
					}
				}
			}
		}
		
		// update numerical bin centers
		i = 0;
		if(clusterNumberBinMatrix != null) {
			int size = clusterNumberBinMatrix.size();
			for(; i < size; i++) {
				ArrayList<Hashtable<String, Integer>> newClusterInfo = clusterNumberBinMatrix.get(i);
				ArrayList<Hashtable<String, Integer>> currClusterInfo = totalClusterNumberBinMatrix.get(i);

				int j = 0;
				int numProps = newClusterInfo.size();
				for(; j < numProps; j++) {
					Hashtable<String, Integer> newPropInfo = newClusterInfo.get(j);
					Hashtable<String, Integer> currPropInfo = currClusterInfo.get(j);

					for(String prop : newPropInfo.keySet()) {
						int count = 1;
						if(currPropInfo.containsKey(prop)) {
							count = currPropInfo.get(prop);
							count += newPropInfo.get(prop);
							currPropInfo.put(prop, count);
						} else {
							currPropInfo.put(prop, count);
						}
					}
				}
			}
		}
	}

	public int[] getTotalNumInstancesInCluster() {
		return totalNumInstancesInCluster;
	}

	public void setTotalNumInstancesInCluster(int[] totalNumInstancesInCluster) {
		this.totalNumInstancesInCluster = totalNumInstancesInCluster;
	}

	public int[] getTotalClusterAssignment() {
		return totalClusterAssignment;
	}

	public void setTotalClusterAssignment(int[] totalClusterAssignment) {
		this.totalClusterAssignment = totalClusterAssignment;
	}

	public ArrayList<ArrayList<Hashtable<String, Integer>>> getTotalClusterCategoricalMatrix() {
		return totalClusterCategoricalMatrix;
	}

	public void setTotalClusterCategoricalMatrix(
			ArrayList<ArrayList<Hashtable<String, Integer>>> totalClusterCategoricalMatrix) {
		this.totalClusterCategoricalMatrix = totalClusterCategoricalMatrix;
	}

	public ArrayList<ArrayList<Hashtable<String, Integer>>> getTotalClusterNumberBinMatrix() {
		return totalClusterNumberBinMatrix;
	}

	public void setTotalClusterNumberBinMatrix(
			ArrayList<ArrayList<Hashtable<String, Integer>>> totalClusterNumberBinMatrix) {
		this.totalClusterNumberBinMatrix = totalClusterNumberBinMatrix;
	}
	
	public void setProcessingQueue(Queue<Integer> processingQueue) {
		this.processingQueue = processingQueue;
	}
	
	public Queue<Integer> getProcessingQueue() {
		return processingQueue;
	}
	
	public void setRemoveEmptyClusters(boolean removeEmptyClusters) {
		this.removeEmptyClusters = removeEmptyClusters;
	}
	
	public boolean getRemoveEmptyClusters() {
		return removeEmptyClusters;
	}
}
