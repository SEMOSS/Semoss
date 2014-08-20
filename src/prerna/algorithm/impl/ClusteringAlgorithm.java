package prerna.algorithm.impl;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
					clusterNumberMatrix = updateClustersNumberProperties(instanceInd, oldClusterForInstance, newClusterForInstance, clusterNumberMatrix, clustersNumInstances);
					clusterCategoryMatrix = updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, clusterCategoryMatrix);
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
			success = false;
			LOGGER.info("Completed Maximum Number of iterations without finding a solution");
		}
		else {
			success = true;
		}
		printOutClusters();
		createClusterRowsForGrid();
		
		//need indices for visualization
		categoryPropIndices = cdp.getCategoryPropIndices();
		numericalPropIndices = cdp.getTotalNumericalPropIndices();
		
		return success;
	}

	/**
	 * Print each cluster with categorical and numerical properties and a list of all instances
	 */
	private void printOutClusters() {
		LOGGER.info("Cluster Results-");

		String[] numericalPropNames = cdp.getNumericalPropNames();
		String[] categoryPropNames = cdp.getCategoryPropNames();

		for(int clusterInd = 0;clusterInd<clustersNumInstances.length;clusterInd++) {
			LOGGER.info("\n\nCluster "+clusterInd+":");

			//print numerical props
			LOGGER.info("\nNumerical Properties- ");
			for(int numberInd=0;numberInd<clusterNumberMatrix[0].length;numberInd++ )
				LOGGER.info(numericalPropNames[numberInd] +": "+ clusterNumberMatrix[clusterInd][numberInd]+", ");				

			//print categorical props
			LOGGER.info("\nCategorical Properties- ");
			for(int numberInd=0;numberInd<clusterCategoryMatrix.get(0).size();numberInd++ ) {
				Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(numberInd);
				String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
				if(!propWithHighFreq.equals("")) {
					int freq = propValHash.get(propWithHighFreq);
					double percent = (1.0*freq)/(1.0*clustersNumInstances[clusterInd])*100;
					DecimalFormat nf = new DecimalFormat("###.##");
					LOGGER.info(categoryPropNames[numberInd] +": "+propWithHighFreq+" "+freq+"(frequency) and "+ nf.format(percent)+"%(percentage), ");	
				}
			}

			//print instances
			LOGGER.info("\nInstances- ");
			for(String instance : instanceIndexHash.keySet()) {
				int clusterAssigned = clustersAssigned[instanceIndexHash.get(instance)];
				if(clusterAssigned == clusterInd)
					LOGGER.info(instance+", ");
			}
		}
	}
	
	/**
	 * Given a specific instance, find the cluster it is most similar to.
	 * For every cluster, call the similarity function between the system and that cluster.
	 * Compare the similarity score of all the clusters and return the one with max similarity.
	 */
	private int findNewClusterForInstance(int instanceInd) throws IllegalArgumentException {
		int clusterIndWithMaxSimilarity = 0;
		double maxSimilarity = cdp.getSimilarityScore(instanceInd,0,clusterNumberMatrix,clusterCategoryMatrix.get(0));
		int clusterIdx;
		for(clusterIdx = 1; clusterIdx < numClusters; clusterIdx++) {
			double similarityForCluster = cdp.getSimilarityScore(instanceInd, clusterIdx, clusterNumberMatrix, clusterCategoryMatrix.get(clusterIdx));
			if(similarityForCluster > maxSimilarity) {
				maxSimilarity = similarityForCluster;
				clusterIndWithMaxSimilarity = clusterIdx;
			}
		}
		return clusterIndWithMaxSimilarity;
	}

	/**
	 * Print each cluster with categorical and numerical properties and a list of all instances
	 */
	private void createClusterRowsForGrid() {
		clusterRows = new ArrayList<Object[]>();

		String[] numericalPropNames = cdp.getNumericalPropNames();
		String[] categoryPropNames = cdp.getCategoryPropNames();

		for(int clusterInd = 0;clusterInd<clustersNumInstances.length;clusterInd++) {
			Object[] clusterRow = new Object[varNames.length+1];
			clusterRow[0] = "";

			for(int propInd=1;propInd<varNames.length;propInd++) {
				String prop = varNames[propInd];
				int categoryInd = ArrayUtilityMethods.calculateIndexOfArray(categoryPropNames, prop);
				if(categoryInd >-1) {
					Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryInd);
					String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
					if(!propWithHighFreq.equals("")) {
						int freq = propValHash.get(propWithHighFreq);
						double percent = (1.0*freq)/(1.0*clustersNumInstances[clusterInd])*100;
						DecimalFormat nf = new DecimalFormat("###.##");
						clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
					}
				} else {
					int numberInd = ArrayUtilityMethods.calculateIndexOfArray(numericalPropNames, prop);
					if(numberInd > -1) {
						clusterRow[propInd] = clusterNumberMatrix[clusterInd][numberInd];
					} else {
						LOGGER.error("No properties matched for " + prop);
					}
				}
			}
			clusterRow[varNames.length] = clusterInd;
			clusterRows.add(clusterRow);
		}		
	}

	private String printMostFrequentProperties(int clusterInd, Hashtable<String, Integer> propValHash) {
		String propWithHighFreq = "";
		int highestFreq = -1;
		for(String propVal : propValHash.keySet()) {
			int freq = propValHash.get(propVal);
			if(freq>highestFreq) {
				highestFreq = freq;
				propWithHighFreq = propVal;
			}
		}
		return propWithHighFreq;
	}

}
