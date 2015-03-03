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

import prerna.algorithm.learning.unsupervised.clustering.ClusterUtilityMethods;

public class DatasetSimilarity {

	private ClusteringNumericalMethods cnm;
	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	
	private String[][] instanceCategoricalMatrix;
	private String[][] instanceNumbericalBinMatrix;
	
	private int size;
	
	private double[] similarityScoresToCluster;
	
	private int numClusters = 1;
	
	private ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix;
	private ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix;
	
	public DatasetSimilarity(ArrayList<Object[]> list, String[] names) {
		ClusterRemoveDuplicates crd = new ClusterRemoveDuplicates(list, names);
		this.masterTable = crd.getRetMasterTable();
		this.masterNames = crd.getRetVarNames();
		
		ClusteringDataProcessor cdp = new ClusteringDataProcessor(masterTable, masterNames);
		instanceCategoricalMatrix = cdp.getCategoricalMatrix();
		instanceNumbericalBinMatrix = cdp.getNumericalBinMatrix();
		cnm = new ClusteringNumericalMethods(instanceNumbericalBinMatrix, instanceCategoricalMatrix, cdp.getNumericalBinOrderingMatrix());
		cnm.setCategoricalWeights(cdp.getCategoricalWeights());
		cnm.setNumericalWeights(cdp.getNumericalWeights());
		
		size = masterTable.size();
	}
	
	public void generateClusterCenters() {
		
		// clustersAssigned is default 0 for each entry, so each instance is in the same cluster
		int[] clustersAssigned = new int[size];
		
		clusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumbericalBinMatrix, clustersAssigned, numClusters);
		//make the cluster category matrix from initial assignments
		clusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoricalMatrix, clustersAssigned, numClusters);
	}
	
	
	public double[] getSimilarityValuesForInstances() {
		
		similarityScoresToCluster = new double[size];
		//iterate through the instance values to generate the similarityScores for each instance
		int j = 0;
		for(; j < size; j++) {
			if(clusterCategoryMatrix != null) {
				if(clusterNumberBinMatrix != null) {
					similarityScoresToCluster[j] = cnm.getSimilarityScore(j, clusterNumberBinMatrix.get(0), clusterCategoryMatrix.get(0));
				} else {
					similarityScoresToCluster[j] = cnm.getSimilarityScore(j, null, clusterCategoryMatrix.get(0));
				}
			} else {
				similarityScoresToCluster[j] = cnm.getSimilarityScore(j, clusterNumberBinMatrix.get(0), null);
			}
		}
		
		return similarityScoresToCluster;
	}
	
	
	
	
}
