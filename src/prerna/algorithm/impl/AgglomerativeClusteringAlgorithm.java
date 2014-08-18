package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AgglomerativeClusteringAlgorithm extends AbstractClusteringAlgorithm{
	
	private static final Logger logger = LogManager.getLogger(ClusteringAlgorithm.class.getName());

	private double[] gammaArr;
	private double[] lambdaArr;
	private int[] numWinsForCluster;
	private double n;
	private int originalNumClusters;
	
	//Constructor
	public AgglomerativeClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
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
		int maxIterations = 100000;
		gammaArr = new double[numClusters];
		lambdaArr = new double[numClusters];
		numWinsForCluster = new int[numClusters];
		originalNumClusters = numClusters;
		
		int i;
		for(i = 0; i < numClusters; i++) {
			gammaArr[i] = lambdaArr[i] = (double) 1 / numClusters;
			numWinsForCluster[i] = 1;
		}
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		while(!noChange && iterationCount <= maxIterations) {
			noChange = true;
			for(String instance : instanceIndexHash.keySet()) {
				int instanceInd = instanceIndexHash.get(instance);
				System.out.println(numClusters);
				int newClustersForInstance = findNewClusterForInstanceAndUpdateScoreValues(instanceInd, iterationCount + 1 + originalNumClusters);
				int oldClusterForInstance = clustersAssigned[instanceInd];
				if(newClustersForInstance != oldClusterForInstance) {
					noChange = false;
					clusterNumberMatrix = updateClustersNumberProperties(instanceInd, oldClusterForInstance, newClustersForInstance, clusterNumberMatrix, clustersNumInstances);
					clusterCategoryMatrix = updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClustersForInstance, clusterCategoryMatrix);
					if(oldClusterForInstance > -1) {
						clustersNumInstances[oldClusterForInstance]--;
						// if there is now nothing in the cluster in cluster, delete it and adjust all indices for variables
						if(clustersNumInstances[oldClusterForInstance] == 0) {
							deleteCluster(oldClusterForInstance);
							numClusters--;
							if(newClustersForInstance > oldClusterForInstance) {
								// since all indices above the old cluster have dropped by one
								newClustersForInstance--;
							}
						}
					}
					clustersNumInstances[newClustersForInstance]++;
					clustersAssigned[instanceInd] = newClustersForInstance;
				}
				
				System.out.println("Iteration number " + (iterationCount+1));
				System.out.println("Number of clusters = " + numClusters);
				System.out.println("Number of instances in each cluster: ");
				for(int j = 0; j < numClusters; j++) {
					System.out.println("\t" + j + "\t" + clustersNumInstances[j]);
				}
				System.out.println("Gamma for each cluster: ");
				for(int j = 0; j < numClusters; j++) {
					System.out.println("\t" + j + "\t" + gammaArr[j]);
				}
				System.out.println("Lambda for each cluster: ");
				for(int j = 0; j < numClusters; j++) {
					System.out.println("\t" + j + "\t" + lambdaArr[j]);
				}
				System.out.println("Number of wins for each cluster: ");
				for(int j = 0; j < numClusters; j++) {
					System.out.println("\t" + j + "\t" + numWinsForCluster[j]);
				}
				iterationCount++;
			}			
		}
		if(iterationCount == maxIterations) {
			success = false;
			System.out.println("Completed Maximum Number of iterations without finding a solution");
		}
		else {
			success = true;
		}
		return success;
	}
	
	/**
	 * Given a specific instance, find the cluster it is most similar to.
	 * For every cluster, call the similarity function between the system and that cluster.
	 * Compare the similarity score of all the clusters and return the one with max similarity.
	 */
	private int findNewClusterForInstanceAndUpdateScoreValues(int instanceInd, int totalCount) throws IllegalArgumentException {
		int[] topTwoClustersWithMaxSimilarity = new int[2];
		// get similarity score
		double similarityToCluster0 = cdp.getSimilarityScore(instanceInd,0,clusterNumberMatrix,clusterCategoryMatrix.get(0));
		double similarityToCluster1 = cdp.getSimilarityScore(instanceInd,1,clusterNumberMatrix,clusterCategoryMatrix.get(1));
		// get 1 - dissimilarity score
		double oneMinusDissimilarityScoreToCluser0 = gammaArr[0] * ( 1 - lambdaArr[0] * similarityToCluster0);
		double oneMinusDissimilarityScoreToCluser1 = gammaArr[1] * ( 1 - lambdaArr[1] * similarityToCluster1);
				
		double minScore;
		double secondScore;
		// compare for first two clusters
		if(oneMinusDissimilarityScoreToCluser1 > oneMinusDissimilarityScoreToCluser0) {
			// double array already initialized to 0 so no need to update first index
			topTwoClustersWithMaxSimilarity[1] = 1;
			minScore = oneMinusDissimilarityScoreToCluser0;
			secondScore = oneMinusDissimilarityScoreToCluser1;
		} else {
			topTwoClustersWithMaxSimilarity[0] = 1;
			topTwoClustersWithMaxSimilarity[1] = 0;
			minScore = oneMinusDissimilarityScoreToCluser1;
			secondScore = oneMinusDissimilarityScoreToCluser0;
		}
		
		// compare first two cluster values to the rest of the clusters
		int clusterIdx;
		for(clusterIdx = 2; clusterIdx < numClusters; clusterIdx++) {
			double similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberMatrix, clusterCategoryMatrix.get(clusterIdx));
			double oneMinusDissimilarityScore = gammaArr[clusterIdx] * ( 1 - lambdaArr[clusterIdx] * similarityForCluster);

			if(oneMinusDissimilarityScore < minScore) {
				// put old value for max similarity in second place similarity score
				secondScore = minScore;
				// put new value for max
				minScore = oneMinusDissimilarityScore;
				// put old index for max in second place
				int oldMaxIdx = topTwoClustersWithMaxSimilarity[0];
				topTwoClustersWithMaxSimilarity[1] = oldMaxIdx;
				// put new value for max index
				topTwoClustersWithMaxSimilarity[0] = clusterIdx;
			}
			if( (oneMinusDissimilarityScore > secondScore) && clusterIdx != topTwoClustersWithMaxSimilarity[0]) {
				// case when similarity is larger than second place but smaller than first place
				secondScore = oneMinusDissimilarityScore;
				topTwoClustersWithMaxSimilarity[1] = clusterIdx;
			}
		}
		
		numWinsForCluster[topTwoClustersWithMaxSimilarity[0]]++;
		//update all gamma values
		int i;
		for(i = 0; i < numClusters; i++) {
			gammaArr[i] = (double) numWinsForCluster[i] / totalCount;
		}
		
		lambdaArr[topTwoClustersWithMaxSimilarity[0]] += n;
		
		double newLamdba = lambdaArr[topTwoClustersWithMaxSimilarity[1]] - n * secondScore;
		if(newLamdba > 0) {
			lambdaArr[topTwoClustersWithMaxSimilarity[1]] = newLamdba;
		} else {
			lambdaArr[topTwoClustersWithMaxSimilarity[1]] = (double) 0;
		}
		
		return topTwoClustersWithMaxSimilarity[0];
	}
	
	public void setN(double n) {
		this.n = n;
	}
	
	/** Updates the cluster number properties matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	 **/
	@Override
	protected final Double[][] updateClustersNumberProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, Double[][] clusterNumberMatrix, int[] clustersNumInstances ) {
		//iterate through every numerical property of instance
		//remove from the old cluster index using old val (avg) * oldNum in cluster
		if(instanceNumberMatrix != null)
		{
			int numNumericalProps = instanceNumberMatrix[0].length;
			int numberIdx;
			if(oldClusterForInstance > -1) {
//				if((clustersNumInstances[oldClusterForInstance] - 1) == 0) {
//					deleteCluster(oldClusterForInstance);
//					numClusters--;
//				} else {			
					for(numberIdx = 0; numberIdx < numNumericalProps; numberIdx++) {
						double numberValForInstance = instanceNumberMatrix[instanceInd][numberIdx];
						// update the old cluster
						double oldNumberValForInstance = clusterNumberMatrix[oldClusterForInstance][numberIdx];
						double valToPut = (oldNumberValForInstance * clustersNumInstances[oldClusterForInstance] - numberValForInstance) /  (clustersNumInstances[oldClusterForInstance] - 1);
						clusterNumberMatrix[oldClusterForInstance][numberIdx] = valToPut;
					}
//				}
			}
			for(numberIdx = 0; numberIdx < numNumericalProps; numberIdx++) {
				// update the new cluster
				double numberValForInstance = instanceNumberMatrix[instanceInd][numberIdx];
				double newClusterValForInstance = clusterNumberMatrix[newClusterForInstance][numberIdx];
				double valToPut = (newClusterValForInstance * clustersNumInstances[newClusterForInstance] + numberValForInstance) /  (clustersNumInstances[newClusterForInstance] + 1);
				clusterNumberMatrix[newClusterForInstance][numberIdx] = valToPut;
			}
			return clusterNumberMatrix;
		}
		return null;
	}

	/** Updates the cluster category property matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	 **/
	@Override
	protected final ArrayList<ArrayList<Hashtable<String,Integer>>> updateClustersCategoryProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix) {
		//iterate through every category property of instance and remove it from the old cluster and put it in the new cluster
		for(int categoryInd=0;categoryInd<instanceCategoryMatrix[instanceInd].length;categoryInd++) {
			String categoryValForInstance = instanceCategoryMatrix[instanceInd][categoryInd];

			if(oldClusterForInstance>-1) {
				//remove the category property from the old cluster
				Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(oldClusterForInstance).get(categoryInd);
				//if the instance's properties are in fact in the clusters properties, remove them, otherwise error.
				if(propValHash.containsKey(categoryValForInstance)) {
					int propCount = propValHash.get(categoryValForInstance);
					propCount --;
					if(propCount == 0) {
						propValHash.remove(categoryValForInstance);
					} else {
						propValHash.put(categoryValForInstance,propCount);
					}
				}
				else{
					System.out.println("ERROR: Property Value of "+categoryValForInstance+"is not included in category "+categoryInd+" for cluster "+oldClusterForInstance);
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
		return clusterCategoryMatrix;
	}
	
	private void deleteCluster(int oldClusterForInstance) {
		System.out.println("Removing cluster " + oldClusterForInstance);
		int i;
		int counter = 0;
		// reduce size of num cluster array
		// reduce size of gammaArr
		// reduce size of lambdaArr
		// reduce size of numWinForCluster
		// reduce size of 
		// remove cluster from numerical matrix
		int[] retNumInstanceArr = new int[numClusters - 1];
		double[] retLambdaArr = new double[numClusters - 1];
		double[] retGammaArr = new double[numClusters - 1];
		int[] retNumWinsForCluster = new int[numClusters - 1];
		Double[][] retClusterNumberMatrix = new Double[numClusters-1][clusterNumberMatrix[0].length];
		for(i = 0; i < numClusters; i++) {
			if(i != oldClusterForInstance) {
				retClusterNumberMatrix[counter] = clusterNumberMatrix[i];
				retNumInstanceArr[counter] = clustersNumInstances[i];
				retLambdaArr[counter] = lambdaArr[i];
				retGammaArr[counter] = gammaArr[i];
				retNumWinsForCluster[counter] = numWinsForCluster[i];
				counter++;
			}
		}
		clustersNumInstances = retNumInstanceArr;
		lambdaArr = retLambdaArr;
		gammaArr = retGammaArr;
		numWinsForCluster = retNumWinsForCluster;
		clusterNumberMatrix = retClusterNumberMatrix;
		
		// delete cluster from categorical prop
		clusterCategoryMatrix.remove(oldClusterForInstance);
		// reassign clusters for all instances due to indexing change
		for(i = 0; i < numInstances; i++) {
			int cluster = clustersAssigned[i];
			if(cluster >= oldClusterForInstance) {
				clustersAssigned[i] = cluster - 1;
			}
		}
	}
}
