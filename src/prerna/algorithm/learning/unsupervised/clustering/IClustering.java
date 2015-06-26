package prerna.algorithm.learning.unsupervised.clustering;

import java.util.List;

import prerna.algorithm.learning.util.Cluster;

public interface IClustering {
	
	/**
	 * Calculates the best cluster for the given index
	 * @param instance						The values for the instance	
	 * @param clusters						The list of clusters to compare the instance to
	 * @return								The index corresponding to the best cluster
	 */
	int findBestClusterForInstance(Object[] instance, List<Cluster> clusters);
	
	/**
	 * Update the cluster center with the given index
	 * @param instance						The values for the instance
	 * @param clusterToAdd					The cluster center that will have the instance added
	 * @param indexOfCluster				The index of the cluster in order to determine if the instance changes cluster centers
	 * @return								boolean of true if instance changed cluster centers, false otherwise
	 */
	boolean updateInstanceIndex(Object[] instance, Cluster clusterToAdd, int indexOfCluster);
	
}
