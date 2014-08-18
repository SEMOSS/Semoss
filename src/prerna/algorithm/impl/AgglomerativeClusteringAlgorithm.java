package prerna.algorithm.impl;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AgglomerativeClusteringAlgorithm extends AbstractClusteringAlgorithm{
	
	private static final Logger logger = LogManager.getLogger(ClusteringAlgorithm.class.getName());

	private double[] gammaArr;
	private double[] lambdaArr;
	private int[] numWinsForCluster;
	private double n;
	private int originalNumClusters;
	PrintWriter writer = null;

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
	public boolean execute() {
		
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
			gammaArr[i] = (double) 1 / numClusters;
			lambdaArr[i] = 0.5;
			numWinsForCluster[i] = 1;
		}
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		Boolean oneCluster = false;
		try {
			writer = new PrintWriter("Clustering_Algorithm_Output_NumCluster_" + numClusters + "_LearningFactor_" + n + ".txt");
			writer.println("Initial Number of clusters = " + numClusters);
			writer.println("Learning Factor = " + n);
			writer.println("");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int counter = 0;
		while(!noChange && iterationCount <= maxIterations) {
			noChange = true;
			writer.println("////////////////////////////////Iteration numer " + counter);
			for(String instance : instanceIndexHash.keySet()) {
				if(numClusters == 1) {
					writer.close();
					oneCluster = true;
					break;
				} else {
					writer.println("Iteration numer: " + counter + ". Looking at instance: " + instance);
					int instanceInd = instanceIndexHash.get(instance);
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
					
					writer.println("Number of clusters = " + numClusters);
					writer.println("Number of instances in each cluster: ");
					for(int j = 0; j < numClusters; j++) {
						writer.println("\t" + j + "\t" + clustersNumInstances[j]);
					}
					writer.println("Gamma for each cluster: ");
					for(int j = 0; j < numClusters; j++) {
						writer.println("\t" + j + "\t" + gammaArr[j]);
					}
					writer.println("Lambda for each cluster: ");
					for(int j = 0; j < numClusters; j++) {
						writer.println("\t" + j + "\t" + lambdaArr[j]);
					}
					writer.println("Number of wins for each cluster: ");
					for(int j = 0; j < numClusters; j++) {
						writer.println("\t" + j + "\t" + numWinsForCluster[j]);
					}
				}
				iterationCount++;	
			}
			counter++;
		}
		if(iterationCount == maxIterations) {
			success = false;
			System.out.println("Completed Maximum Number of iterations without finding a solution");
		}
		else if(oneCluster){
			success = true;
			writer.close();
		} else {
			success = true;
			writer.close();
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
		
		double newLambdaWinner = lambdaArr[topTwoClustersWithMaxSimilarity[0]] + n;
		if(newLambdaWinner > 1) {
			lambdaArr[topTwoClustersWithMaxSimilarity[0]] = (double) 1;
		} else {
			lambdaArr[topTwoClustersWithMaxSimilarity[0]] = newLambdaWinner;
		}
		
		double newLamdbaRunnerUp = lambdaArr[topTwoClustersWithMaxSimilarity[1]] - n * secondScore;
		if(newLamdbaRunnerUp > 0) {
			lambdaArr[topTwoClustersWithMaxSimilarity[1]] = newLamdbaRunnerUp;
		} else {
			lambdaArr[topTwoClustersWithMaxSimilarity[1]] = (double) 0;
		}
		
		writer.println("Found min cluster: #" + topTwoClustersWithMaxSimilarity[0] + " with value of " + minScore);
		writer.println("Found runner up cluster: #" + topTwoClustersWithMaxSimilarity[1] + " with value of " + secondScore);

		return topTwoClustersWithMaxSimilarity[0];
	}
	
	public void setN(double n) {
		this.n = n;
	}
	
	private void deleteCluster(int oldClusterForInstance) {
		writer.println("Removing cluster " + oldClusterForInstance);
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
