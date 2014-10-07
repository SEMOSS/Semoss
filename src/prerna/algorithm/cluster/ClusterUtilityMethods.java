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
						//add the category properties to the new cluster
						Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryIdx);
						if(propValHash.contains(categoryValForInstance)) {
							int count = propValHash.get(categoryValForInstance);
							propValHash.put(categoryValForInstance, ++count);
						} else {
							propValHash.put(categoryValForInstance, 1);
						}
						clusterCategoryMatrix.get(clusterInd).set(categoryIdx, propValHash);
					}
				}
			}
			return clusterCategoryMatrix;
		}
		return null;
	}
	
//	/** Updates the cluster number properties matrix for the instance that is switching clusters
//	 * This removes the instance's properties from the old clusters properties.
//	 * This add the instance's properties to the new cluster's properties.
//	 **/
//	protected Double[][] updateClustersNumberProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, Double[][] clusterNumberMatrix, int[] clustersNumInstances ) {
//		//iterate through every numerical property of instance
//		//remove from the old cluster index using old val (avg) * oldNum in cluster
//		if(instanceNumberMatrix != null)
//		{
//			int numNumericalProps = instanceNumberMatrix[0].length;
//			int numberIdx;
//			for(numberIdx = 0; numberIdx < numNumericalProps; numberIdx++) {
//				double numberValForInstance = instanceNumberMatrix[instanceInd][numberIdx];
//				// satisfies condition if the instance is already in a cluster
//				if(oldClusterForInstance > -1) {
//					// update the old cluster
//					double oldNumberValForInstance = clusterNumberMatrix[oldClusterForInstance][numberIdx];
//					double valToPut = (oldNumberValForInstance * clustersNumInstances[oldClusterForInstance] - numberValForInstance) /  (clustersNumInstances[oldClusterForInstance] - 1);
//					clusterNumberMatrix[oldClusterForInstance][numberIdx] = valToPut;
//				}
//				// update the new cluster
//				double newClusterValForInstance = clusterNumberMatrix[newClusterForInstance][numberIdx];
//				double valToPut = (newClusterValForInstance * clustersNumInstances[newClusterForInstance] + numberValForInstance) /  (clustersNumInstances[newClusterForInstance] + 1);
//				clusterNumberMatrix[newClusterForInstance][numberIdx] = valToPut;
//			}
//			return clusterNumberMatrix;
//		}
//		return null;
//	}

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
		if(clusterCategoryMatrix != null)
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
		if(clusterCategoryMatrix != null) {
			if(clusterNumberBinMatrix != null) {
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
			if(clusterCategoryMatrix != null) {
				if(clusterNumberBinMatrix != null) {
				similarityForCluster = cnm.getSimilarityScore(instanceInd, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
				} else {
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
