/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ClusteringOptimization extends PartitionedClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringOptimization.class.getName());
	
	public ClusteringOptimization(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}
	
	private Hashtable<Integer, Double> clusterScoreHash = new Hashtable<Integer, Double>();
	
	public void runGoldenSelectionForNumberOfClusters(int start, int end) {		
		int a = start;
		int b = end;
		double phi = (double) (1 + Math.sqrt(5)) / 2;
		int x1 = (int) Math.round((phi - 1)*a + (2-phi)*b);
		int x2 = (int) Math.round((2-phi)*a + (phi - 1)*b);
		
		double startVal = getClusterNumScore(x1);
		double endVal = getClusterNumScore(x2);
		
		while(Math.abs(b - a) > 1) {
			if(startVal < endVal) {
				a = x1;
				x1 = x2;
				x2 = (int) Math.round((2-phi)*a + (phi-1)*b);
				
				startVal = getClusterNumScore(x1);
				endVal = getClusterNumScore(x2);
			} else {
				b = x2;
				x2 = x1;
				x1 = (int) Math.round((phi-1)*a + (2-phi)*b);
				
				startVal = getClusterNumScore(x1);
				endVal = getClusterNumScore(x2);
			}
		}
		
		if(clusterScoreHash.get(a) < clusterScoreHash.get(b)) {
			numClusters = b;
		} else {
			numClusters = a;
		}
	}
	
	private double getClusterNumScore(int numClusterToTest) {
		if(clusterScoreHash.containsKey(numClusterToTest)) {
			return clusterScoreHash.get(numClusterToTest);
		}
		
		LOGGER.info("Running algorithm for clusterNum: " + numClusterToTest);
		setNumClusters(numClusterToTest);
		generateInitialClusters();
		execute();
		double instanceToClusterSim = calculateFinalInstancesToClusterSimilarity();
		double clusterToClusterSim = calculateFinalTotalClusterToClusterSimilarity();
		double sum = instanceToClusterSim + clusterToClusterSim;
		double items = numInstances + (double) (numClusterToTest * (numClusterToTest-1) /2);
		double average = sum/items;
		LOGGER.info("Determined cluster score: " + average);
		clusterScoreHash.put(numClusterToTest, average);
		
		return average;
	}
	
	public void clearClusterScoreHash() {
		clusterScoreHash.clear();
	}

	public int getNumClusters(){
		return numClusters;
	}
}
