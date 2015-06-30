package prerna.algorithm.learning.unsupervised.clustering;

import java.util.List;
import java.util.Map;

import prerna.algorithm.learning.util.Cluster;

public interface IClustering {
	
	/**
	 * Calculates the best cluster for the given index
	 * @param instance						The values for the instance	
	 * @param attributeNames				The names of all the attributes corresponding to the instance array
	 * @param isNumeric						boolean array saying which parameters are numeric vs. string
	 * @param instanceIndex					The index in the instance array saying which value corresponds to the instance unique id
	 * @param clusters						The list of clusters to compare the instance to
	 * @return								The index corresponding to the best cluster
	 */
	int findBestClusterForInstance(Object[] instance, String[] attributeNames, boolean[] isNumeric, int instanceIndex, List<Cluster> clusters);
	
	/**
	 * Update the cluster center with the given instance
	 * @param instance						The values for the instance
	 * @param attributeNames				The names of all the attributes corresponding to the instance array
	 * @param isNumeric						boolean array saying which parameters are numeric vs. string
	 * @param clusterToAdd					The cluster center that will have the instance added
	 */
	void updateInstanceIndex(Object[] instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToAdd);
	
	/**
	 * Determine if the inputed instance has changed clusters in current iteration
	 * @param results						Map containing all instance to cluster relationships
	 * @param instanceName					The name of the instance
	 * @param bestCluster					The best cluster for the instance in the current algorithm iteration
	 * @return								boolean of true if instance changed cluster centers, false otherwise
	 */
	boolean isInstanceChangedCluster(Map<String, Integer> results, String instanceName, int bestCluster);
}
