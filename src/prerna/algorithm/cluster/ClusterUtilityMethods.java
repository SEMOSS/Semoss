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
package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class ClusterUtilityMethods {

	private static final Logger LOGGER = LogManager.getLogger(ClusterUtilityMethods.class.getName());
	
	private ClusterUtilityMethods() {
		
	}
	
	/** Creates the initial cluster category property matrix.
	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
	 **/
	public static ArrayList<ArrayList<Hashtable<String,Integer>>> createClustersCategoryProperties(
			String[][] instanceCategoryMatrix, 
			int[] clustersAssigned, 
			int numClusters) 
	{
		if(instanceCategoryMatrix != null) {
			int numCategoricalProp = instanceCategoryMatrix[0].length;

			//iterate through every category property of instance and remove it from the old cluster and put it in the new cluster
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix = new ArrayList<ArrayList<Hashtable<String,Integer>>>();
			int clusterInd;
			for(clusterInd = 0; clusterInd < numClusters; clusterInd++) {
				ArrayList<Hashtable<String,Integer>> listForCluster = new ArrayList<Hashtable<String,Integer>>();
				int categoryIdx;
				for(categoryIdx = 0; categoryIdx < numCategoricalProp; categoryIdx++) {
					Hashtable<String,Integer> propHash = new Hashtable<String,Integer>();
					listForCluster.add(propHash);
				}
				clusterCategoryMatrix.add(listForCluster);
			}
			//iterate through every instance
			int instanceIdx;
			for(instanceIdx = 0; instanceIdx < clustersAssigned.length; instanceIdx++) {
				clusterInd = clustersAssigned[instanceIdx];
				//if the instance is assigned to a cluster, then put its categorical properties in the cluster category properties Matrix
				if(clusterInd > -1) {
					int categoryIdx;
					for(categoryIdx = 0; categoryIdx < numCategoricalProp; categoryIdx++) {
						String categoryValForInstance = instanceCategoryMatrix[instanceIdx][categoryIdx];
						if(categoryValForInstance != null) {
							//add the category properties to the new cluster
							Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryIdx);
							if(propValHash.containsKey(categoryValForInstance)) {
								int count = propValHash.get(categoryValForInstance);
								propValHash.put(categoryValForInstance, ++count);
							} else {
								propValHash.put(categoryValForInstance, 1);
							}
							clusterCategoryMatrix.get(clusterInd).set(categoryIdx, propValHash);
						}
					}
				}
			}
			return clusterCategoryMatrix;
		}
		return null;
	}
	
	/** Updates the cluster category property matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	 **/
	public static ArrayList<ArrayList<Hashtable<String,Integer>>> updateClustersCategoryProperties(
			int instanceInd, 
			int oldClusterForInstance, 
			int newClusterForInstance, 
			String[][] instanceData, 
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix) 
	{
		//iterate through every category property of instance and remove it from the old cluster and put it in the new cluster
		if(clusterCategoryMatrix != null && !clusterCategoryMatrix.isEmpty())
		{
			for(int categoryInd=0;categoryInd<instanceData[instanceInd].length;categoryInd++) {
				String categoryValForInstance = instanceData[instanceInd][categoryInd];
	
				if(oldClusterForInstance>-1) {
					//remove the category property from the old cluster
					Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(oldClusterForInstance).get(categoryInd);
					//if the instance's properties are in fact in the clusters properties, remove them, otherwise error.
					if(propValHash.containsKey(categoryValForInstance)) {
						int propCount = propValHash.get(categoryValForInstance);
						propCount--;
						if(propCount == 0) {
							propValHash.remove(categoryValForInstance);
							clusterCategoryMatrix.get(oldClusterForInstance).set(categoryInd, propValHash);
						} else {
							propValHash.put(categoryValForInstance,propCount);
							clusterCategoryMatrix.get(oldClusterForInstance).set(categoryInd, propValHash);
						}
					}
					else {
						LOGGER.info("ERROR: Property Value of "+categoryValForInstance+"is not included in category "+categoryInd+" for cluster "+oldClusterForInstance);
					}
				}
				//add the category properties to the new cluster
				Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(newClusterForInstance).get(categoryInd);
				//if there is already a count going for the same property as the instance, add to it, otherwise create a new hash entry
				if(propValHash.containsKey(categoryValForInstance)) {
					int propCount = propValHash.get(categoryValForInstance);
					propCount ++;
					propValHash.put(categoryValForInstance,propCount);
					clusterCategoryMatrix.get(newClusterForInstance).set(categoryInd, propValHash);
				}
				else{
					propValHash.put(categoryValForInstance, 1);
					clusterCategoryMatrix.get(newClusterForInstance).set(categoryInd, propValHash);
				}
			}
		}
		return clusterCategoryMatrix;
	}
	
	
	/**
	 * Given a specific instance, find the cluster it is most similar to.
	 * For every cluster, call the similarity function between the system and that cluster.
	 * Compare the similarity score of all the clusters and return the one with max similarity.
	 */
	public static int findNewClusterForInstance(
			ClusteringNumericalMethods cnm,
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix,
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix,
			int[] numInstancesInCluster,
			int instanceInd) 
			throws IllegalArgumentException 
	{
		int numClusters = numInstancesInCluster.length;
		
		int clusterIndWithMaxSimilarity = 0;
		double maxSimilarity;
		if(clusterCategoryMatrix != null && !clusterCategoryMatrix.isEmpty()) {
			if(clusterNumberBinMatrix != null && !clusterNumberBinMatrix.isEmpty()) {
				maxSimilarity = cnm.getSimilarityScore(instanceInd, clusterNumberBinMatrix.get(0), clusterCategoryMatrix.get(0));
			} else {
				maxSimilarity = cnm.getSimilarityScore(instanceInd, null, clusterCategoryMatrix.get(0));
			}
		} else {
			maxSimilarity = cnm.getSimilarityScore(instanceInd, clusterNumberBinMatrix.get(0), null);
		}
		
		int clusterIdx;
		for(clusterIdx = 1; clusterIdx < numClusters; clusterIdx++) {
			double similarityForCluster;
			if(clusterCategoryMatrix != null && !clusterCategoryMatrix.isEmpty()) {
				if(clusterNumberBinMatrix != null && !clusterNumberBinMatrix.isEmpty()) {
					similarityForCluster = cnm.getSimilarityScore(instanceInd, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
				} else {
					if(clusterCategoryMatrix.size() <= clusterIdx) {
						LOGGER.info("error");
					}
					similarityForCluster = cnm.getSimilarityScore(instanceInd, null, clusterCategoryMatrix.get(clusterIdx));
				}
			} else {
				similarityForCluster = cnm.getSimilarityScore(instanceInd, clusterNumberBinMatrix.get(clusterIdx), null);
			}
			if(similarityForCluster > maxSimilarity) {
				maxSimilarity = similarityForCluster;
				clusterIndWithMaxSimilarity = clusterIdx;
			}
		}
		
		// if there is no similarity to any cluster, see if any cluster is empty and put there
		if(maxSimilarity == 0) {
			int i;
			for(i = 0; i < numClusters; i++) {
				if(numInstancesInCluster[i] == 0) {
					//return the first empty cluster
					return numInstancesInCluster[i];
				}
			}
		}
		
		return clusterIndWithMaxSimilarity;
	}
	
//	/**
//	 * Given a specific instance, find the cluster it is most similar to.
//	 * For every cluster, call the similarity function between the system and that cluster.
//	 * Compare the similarity score of all the clusters and return the one with max similarity.
//	 */
//	private int findNewClusterForInstance(int instanceInd) throws IllegalArgumentException {
//		int clusterIndWithMaxSimilarity = 0;
//		double maxSimilarity;
//		if(clusterCategoryMatrix != null) {
//			maxSimilarity = cdp.getSimilarityScore(instanceInd, 0, clusterNumberMatrix, clusterCategoryMatrix.get(0));
//		} else {
//			maxSimilarity = cdp.getSimilarityScore(instanceInd, 0, clusterNumberMatrix, null);
//		}
//		int clusterIdx;
//		for(clusterIdx = 1; clusterIdx < numClusters; clusterIdx++) {
//			double similarityForCluster;
//			if(clusterCategoryMatrix != null) {
//				similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberMatrix, clusterCategoryMatrix.get(clusterIdx));
//			} else {
//				similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberMatrix, null);
//			}
//			if(similarityForCluster > maxSimilarity) {
//				maxSimilarity = similarityForCluster;
//				clusterIndWithMaxSimilarity = clusterIdx;
//			}
//		}
//		return clusterIndWithMaxSimilarity;
//	}
	
}
