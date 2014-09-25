package prerna.algorithm.cluster;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

/** Generic clustering algorithm to cluster instances based on their categorical and numerical properties.
 * 
 */
public class ClusteringAlgorithm extends AbstractClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringAlgorithm.class.getName());

	//Constructor
	public ClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}

	/** Performs the clustering based off of the instance's categorical and numerical properties.
	 * These properties are pulled from the instanceCategoryMatrix and instanceNumberMatrix, that are filled prior to start.
	 * The final cluster each instance is assigned to is stored in clustersAssigned.
	 * The categorical and numerical properties for each cluster based on the instances it contains are stored in clusterCategoryMatrix and clusterNumberMatrix.
	 * The number of instances in each cluster is stored in clustersNumInstances.
	 */
	@Override
	public boolean execute() throws IllegalArgumentException {
		
		setUpAlgorithmVariables();
		
		boolean success;
		boolean noChange = false;
		int iterationCount = 0;
		int maxIterations = 1000000;
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		while(!noChange && iterationCount <= maxIterations) {
			noChange = true;
			for(String instance : instanceIndexHash.keySet()) {
				int instanceInd = instanceIndexHash.get(instance);
				int newClusterForInstance = findNewClusterForInstance(instanceInd);
				int oldClusterForInstance = clustersAssigned[instanceInd];
				if(newClusterForInstance != oldClusterForInstance) {
					noChange = false;
					//TODO: delete
//					clusterNumberMatrix = updateClustersNumberProperties(instanceInd, oldClusterForInstance, newClusterForInstance, clusterNumberMatrix, clustersNumInstances);
					clusterNumberBinMatrix = updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceNumberBinMatrix, clusterNumberBinMatrix);
					clusterCategoryMatrix = updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceCategoryMatrix, clusterCategoryMatrix);
					if(oldClusterForInstance > -1) {
						clustersNumInstances[oldClusterForInstance]--;
					}
					clustersNumInstances[newClusterForInstance]++;
					clustersAssigned[instanceInd] = newClusterForInstance;
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
			int i;
			int nonEmptyClusterCount = clustersNumInstances.length;
			int counter = 0;
			for(i = 0; i < nonEmptyClusterCount; i++) {
				if(clustersNumInstances[i] == 0) {
					if(clusterNumberBinMatrix != null) {
						clusterNumberBinMatrix.remove(i - counter);
					}
					if(clusterCategoryMatrix != null) {
						clusterCategoryMatrix.remove(i - counter);
					}
					counter++;
				}
			}
			clustersNumInstances = ArrayUtilityMethods.removeAllZeroValues(clustersNumInstances);
		}
		
		
//		printOutClusters();
		createClusterRowsForGrid();
		
		//need indices for visualization
		categoryPropIndices = cdp.getCategoryPropIndices();
		numericalPropIndices = cdp.getTotalNumericalPropIndices();
		
		return success;
	}
	
	/**
	 * Given a specific instance, find the cluster it is most similar to.
	 * For every cluster, call the similarity function between the system and that cluster.
	 * Compare the similarity score of all the clusters and return the one with max similarity.
	 */
	private int findNewClusterForInstance(int instanceInd) throws IllegalArgumentException {
		int clusterIndWithMaxSimilarity = 0;
		double maxSimilarity;
		if(clusterCategoryMatrix != null) {
			if(clusterNumberBinMatrix != null) {
				maxSimilarity = cdp.getSimilarityScore(instanceInd, 0, clusterNumberBinMatrix.get(0), clusterCategoryMatrix.get(0));
			} else {
				maxSimilarity = cdp.getSimilarityScore(instanceInd, 0, null, clusterCategoryMatrix.get(0));
			}
		} else {
			maxSimilarity = cdp.getSimilarityScore(instanceInd, 0, clusterNumberBinMatrix.get(0), null);
		}
		int clusterIdx;
		for(clusterIdx = 1; clusterIdx < numClusters; clusterIdx++) {
			double similarityForCluster;
			if(clusterCategoryMatrix != null) {
				if(clusterNumberBinMatrix != null) {
				similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
				} else {
					similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, null, clusterCategoryMatrix.get(clusterIdx));
				}
			} else {
				similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberBinMatrix.get(clusterIdx), null);
			}
			if(similarityForCluster > maxSimilarity) {
				maxSimilarity = similarityForCluster;
				clusterIndWithMaxSimilarity = clusterIdx;
			}
		}
		
		// if there is no similarity to any cluster, see if any cluster is empty and put there
		if(maxSimilarity == 0) {
			int i;
			int size = clustersNumInstances.length;
			for(i = 0; i < size; i++) {
				if(clustersNumInstances[i] == 0) {
					//return the first empty cluster
					return clustersNumInstances[i];
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
