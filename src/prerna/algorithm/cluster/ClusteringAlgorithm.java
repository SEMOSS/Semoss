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
		
		if(numClusters > numInstances) {
			return success = false;
		}
		
		setAlgorithmVariables();
		
		boolean noChange = false;
		int iterationCount = 0;
		int maxIterations = 1000000;
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		while(!noChange && iterationCount <= maxIterations) {
			noChange = true;
			for(String instance : instanceIndexHash.keySet()) {
				int instanceInd = instanceIndexHash.get(instance);
				int newClusterForInstance = ClusterUtilityMethods.findNewClusterForInstance(cnm, clusterCategoryMatrix, clusterNumberBinMatrix, numInstancesInCluster, instanceInd);
				int oldClusterForInstance = clustersAssigned[instanceInd];
				if(newClusterForInstance != oldClusterForInstance) {
					noChange = false;
//					clusterNumberMatrix = updateClustersNumberProperties(instanceInd, oldClusterForInstance, newClusterForInstance, clusterNumberMatrix, clustersNumInstances);
					clusterNumberBinMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceNumberBinMatrix, clusterNumberBinMatrix);
					clusterCategoryMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(instanceInd, oldClusterForInstance, newClusterForInstance, instanceCategoryMatrix, clusterCategoryMatrix);
					if(oldClusterForInstance > -1) {
						numInstancesInCluster[oldClusterForInstance]--;
					}
					numInstancesInCluster[newClusterForInstance]++;
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
			int nonEmptyClusterCount = numInstancesInCluster.length;
			int counter = 0;
			for(i = 0; i < nonEmptyClusterCount; i++) {
				if(numInstancesInCluster[i] == 0) {
					if(clusterNumberBinMatrix != null) {
						clusterNumberBinMatrix.remove(i - counter);
					}
					if(clusterCategoryMatrix != null) {
						clusterCategoryMatrix.remove(i - counter);
					}
					int j;
					int size = clustersAssigned.length;
					for(j = 0; j < size; j++) {
						if(clustersAssigned[j] > i - counter) {
							clustersAssigned[j]--;
						}
					}
					counter++;
				}
			}
			numInstancesInCluster = ArrayUtilityMethods.removeAllZeroValues(numInstancesInCluster);
			numClusters = numInstancesInCluster.length;
		}
		
//		printOutClusters();
		createClusterSummaryRowsForGrid();
		
		//need indices for visualization
		categoryPropIndices = cdp.getCategoryPropIndices();
		numericalPropIndices = cdp.getTotalNumericalPropIndices();
		
		return success;
	}
}
