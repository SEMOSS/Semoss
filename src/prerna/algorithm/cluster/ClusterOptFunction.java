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

import java.util.HashMap;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ClusterOptFunction implements UnivariateFunction{

	private static final Logger LOGGER = LogManager.getLogger(ClusterOptFunction.class.getName());
	
	private AbstractClusteringAlgorithm clusteringOptimization;
	private HashMap<Integer, Double> values = new HashMap<Integer, Double>();
	private int numInstances;
	
	@Override
	public double value(double arg0) {
		int numClusterFloor = (int) Math.floor(arg0);
		int numClusterCeil = (int) Math.ceil(arg0);
		
		double avg1;
		double avg2;
		
		if(values.containsKey(numClusterFloor)) {
			avg1 = values.get(numClusterFloor);
		} else {
			avg1 = value(numClusterFloor);
		}
		
		if(values.containsKey(numClusterCeil)) {
			avg2 = values.get(numClusterCeil);
		} else {
			avg2 = value(numClusterCeil);
		}
		// assume linear relationship for similarity density between individual points
		double diff = avg2 - avg1;
		double ratio = arg0 - (int) arg0;
		double retVal = avg1 + diff*ratio;

		return retVal;
	}
	
	public double value(int arg0) {
		clusteringOptimization.setNumClusters(arg0);
		((PartitionedClusteringAlgorithm) clusteringOptimization).generateInitialClusters();
		clusteringOptimization.execute();
		double instanceToClusterSim = clusteringOptimization.calculateFinalInstancesToClusterSimilarity();
		double clusterToClusterSim = clusteringOptimization.calculateFinalTotalClusterToClusterSimilarity();
		double sum = instanceToClusterSim + clusterToClusterSim;
		double items = numInstances + (double) (arg0 * (arg0-1) /2);
		double average = sum/items;		
		values.put(arg0, average);

		return average;
	}
	
	public void setClusterOptimization(ClusteringOptimization clusteringOptimization) {
		this.clusteringOptimization = clusteringOptimization;
	}
	
	public void setNumInstances(int numInstances){
		this.numInstances = numInstances;
	}
}
