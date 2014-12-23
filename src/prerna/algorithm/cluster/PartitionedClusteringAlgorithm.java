package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

public class PartitionedClusteringAlgorithm extends ClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(PartitionedClusteringAlgorithm.class.getName());
	
	private int[] masterOrderedOriginalClusterAssingment;
	
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterCategoricalMatrix;
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterNumberBinMatrix;
	
	public PartitionedClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}

	public void generateBaseClusterInformation(int maximumNumberOfClusters) {
		cnm = new ClusteringNumericalMethods(instanceNumberBinMatrix, instanceCategoryMatrix, instanceNumberBinOrderingMatrix);
		cnm.setCategoricalWeights(categoricalWeights);
		cnm.setNumericalWeights(numericalWeights);
		
		randomlyAssignClusters(numInstances, maximumNumberOfClusters); // creates clusterAssignment and orderedOriginalClusterAssignment
		masterOrderedOriginalClusterAssingment = orderedOriginalClusterAssignment.clone();
	}
	
	public void generateInitialClusters() {
		LOGGER.info("Generating Initial Clustes ");
		numInstancesInCluster = initalizeClusterMatrix(numClusters);
		
		instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
		
		randomlyAssignClustersFromValues(numInstances, numClusters);  // creates clusterAssignment and orderedOriginalClusterAssignment
		//make the custer number matrix from initial assignments
		initialClusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clusterAssignment, numClusters);
		//make the cluster category matrix from initial assignments
		initialClusterCategoricalMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clusterAssignment, numClusters);
		
		clusterCategoryMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>(numClusters);
		clusterNumberBinMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>(numClusters);
		copyHash(initialClusterCategoricalMatrix, clusterCategoryMatrix);
		copyHash(initialClusterNumberBinMatrix, clusterNumberBinMatrix);
	}
	
	public void randomlyAssignClustersFromValues(int numInstances, int numClusters) {
		clusterAssignment = new int[numInstances];
		orderedOriginalClusterAssignment = new int[numClusters];
		int i = 0;
		for(; i < numInstances; i++) {
			clusterAssignment[i] = -1;
		}
		
		i = 0;
		for(; i < numClusters; i++) {
			int position = masterOrderedOriginalClusterAssingment[i];
			clusterAssignment[position] = i;
			orderedOriginalClusterAssignment[i] = position;
		}
	}
	
	@Override
	public boolean execute() {
		
		success = true;
		int numRows = 0;
		int numCols = 0;
		if(instanceNumberBinMatrix != null) {
			numRows = instanceNumberBinMatrix.length;
			numCols += instanceNumberBinMatrix[0].length;
		}
		if(instanceCategoryMatrix != null) {
			numRows = instanceCategoryMatrix.length;
			numCols += instanceCategoryMatrix[0].length;
		}
		int numPartitions = (int) Math.ceil((double) numRows*numCols/40_000);
		int partitionSize = (int) Math.ceil((double) numRows/numPartitions);
		
		// if not all data is divided evenly, add those instances to first run
		int straglers = (numPartitions*partitionSize) % numRows;
		
//		int partionSize = 500;
		int i = 0;
		
		double[] categoricalWeights = cdp.getCategoricalWeights();
		double[] numericalWeights = cdp.getNumericalWeights();
		numInstancesInCluster = initalizeClusterMatrix(numClusters);
		
		AbstractClusteringAlgorithm partitonedAlg = new ClusteringAlgorithm();
		partitonedAlg.setVarNames(varNames);
		partitonedAlg.setCategoricalWeights(categoricalWeights);
		partitonedAlg.setNumericalWeights(numericalWeights);
		((ClusteringAlgorithm) partitonedAlg).setRemoveEmptyClusters(false);
		partitonedAlg.setCategoryPropNames(categoryPropNames);
		partitonedAlg.setNumericalPropNames(numericalPropNames);
		partitonedAlg.setNumericalPropIndices(numericalPropIndices);
		partitonedAlg.setCategoricalPropIndices(categoryPropIndices);
		partitonedAlg.setNumInstances(partitionSize);
		partitonedAlg.setNumClusters(numClusters);
		partitonedAlg.setNumInstancesInCluster(numInstancesInCluster);
		for(; i < numInstances; i += partitionSize) {
			int val = i+partitionSize;
			if(i == 0) {
				val += straglers;
			}
			String[][] partitonedNumberBinMatrix = null;
			String[][] partitonedCategoryMatrix = null;
			if(instanceNumberBinMatrix != null) {
				partitonedNumberBinMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(instanceNumberBinMatrix, i, val);
			}
			if(instanceCategoryMatrix != null) {
				partitonedCategoryMatrix = ArrayUtilityMethods.getRowRangeFromMatrix(instanceCategoryMatrix, i, val);
			}
			int[] partitonedClustersAssignment = Arrays.copyOfRange(clusterAssignment, i, val);
			partitonedAlg.setRecreateStartingClusterValues(false);
			partitonedAlg.setClusterAssignment(partitonedClustersAssignment);
			partitonedAlg.setInstanceCategoricalMatrix(partitonedCategoryMatrix);
			partitonedAlg.setInstanceNumberBinMatrix(partitonedNumberBinMatrix);
			partitonedAlg.setInstanceNumberBinOrderingMatrix(instanceNumberBinOrderingMatrix);
			
			// need to create new objects so original bin matrix does not get changes
			ArrayList<ArrayList<Hashtable<String, Integer>>> partitionedInitialClusterCategoricalMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>();
			copyHash(initialClusterCategoricalMatrix, partitionedInitialClusterCategoricalMatrix);
			ArrayList<ArrayList<Hashtable<String, Integer>>> partitionedClusterNumberBinMatrix = new ArrayList<ArrayList<Hashtable<String, Integer>>>();
			copyHash(initialClusterNumberBinMatrix, partitionedClusterNumberBinMatrix);
			
			partitonedAlg.setClusterCategoricalMatrix(partitionedInitialClusterCategoricalMatrix);
			partitonedAlg.setClusterNumberBinMatrix(partitionedClusterNumberBinMatrix);

			success = partitonedAlg.execute();
			if(success == false) {
				return success;
			}
			
			ArrayList<ArrayList<Hashtable<String, Integer>>> newCategoricalCluster = partitonedAlg.getClusterCategoricalMatrix();
			ArrayList<ArrayList<Hashtable<String, Integer>>> newNumbericalBinCluster = partitonedAlg.getClusterNumberBinMatrix();
			int[] partionedClusterAssignment = partitonedAlg.getClusterAssignment();
			int[] partionedNumInstancesInCluster = partitonedAlg.getNumInstancesInCluster();
			numInstancesInCluster = updateArray(partionedNumInstancesInCluster, numInstancesInCluster);
			if(i == 0) {
				System.arraycopy(partionedClusterAssignment, 0, clusterAssignment, i, partitionSize + straglers);
				i += straglers;
			} else {
				System.arraycopy(partionedClusterAssignment, 0, clusterAssignment, i, partitionSize);
			}
			combineResults(newCategoricalCluster, newNumbericalBinCluster);
		}
		createClusterSummaryRowsForGrid();
		
		return success;
	}
	
	private int[] updateArray(int[] original, int[] copyToArr) {
		int i = 0;
		int size = original.length;
		for(; i < size; i++) {
			copyToArr[i] += original[i];
		}
		
		return copyToArr;
	}
	
	private void combineResults(ArrayList<ArrayList<Hashtable<String, Integer>>> newCategoricalCluster, ArrayList<ArrayList<Hashtable<String, Integer>>> newNumbericalBinCluster) {
		// update categorical centers
		int i = 0;
		if(newCategoricalCluster != null) {
			int size = newCategoricalCluster.size();
			for(; i < size; i++) {
				ArrayList<Hashtable<String, Integer>> newClusterInfo = newCategoricalCluster.get(i);
				ArrayList<Hashtable<String, Integer>> currClusterInfo = clusterCategoryMatrix.get(i);
				
				int j = 0;
				int numProps = newClusterInfo.size();
				for(; j < numProps; j++) {
					Hashtable<String, Integer> newPropInfo = newClusterInfo.get(j);
					Hashtable<String, Integer> currPropInfo = currClusterInfo.get(j);
					
					for(String prop : newPropInfo.keySet()) {
						int count = newPropInfo.get(prop);
						if(currPropInfo.containsKey(prop)) {
							count += currPropInfo.get(prop);
							currPropInfo.put(prop, count);
						} else {
							currPropInfo.put(prop, count);
						}
					}
				}
			}
		}
		
		// update numerical bin centers
		i = 0;
		if(newNumbericalBinCluster != null) {
			int size = newNumbericalBinCluster.size();
			for(; i < size; i++) {
				ArrayList<Hashtable<String, Integer>> newClusterInfo = newNumbericalBinCluster.get(i);
				ArrayList<Hashtable<String, Integer>> currClusterInfo = clusterNumberBinMatrix.get(i);
				
				int j = 0;
				int numProps = newClusterInfo.size();
				for(; j < numProps; j++) {
					Hashtable<String, Integer> newPropInfo = newClusterInfo.get(j);
					Hashtable<String, Integer> currPropInfo = currClusterInfo.get(j);
					
					for(String prop : newPropInfo.keySet()) {
						int count = 1;
						if(currPropInfo.containsKey(prop)) {
							count = currPropInfo.get(prop);
							count += newPropInfo.get(prop);
							currPropInfo.put(prop, count);
						} else {
							currPropInfo.put(prop, count);
						}
					}
				}
			}
		}
	}
	
	private void copyHash(final ArrayList<ArrayList<Hashtable<String, Integer>>> original, ArrayList<ArrayList<Hashtable<String, Integer>>> copyToHash) {
		if(original != null) {
			for(ArrayList<Hashtable<String, Integer>> innerList : original) {
				ArrayList<Hashtable<String, Integer>> copyList = new ArrayList<Hashtable<String, Integer>>();
				for(Hashtable<String, Integer> hash : innerList) {
					Hashtable<String, Integer> copyHash = new Hashtable<String, Integer>();
					for(String prop : hash.keySet()) {
						int val = hash.get(prop);
						copyHash.put(prop, val);
					}
					copyList.add(copyHash);
				}
				copyToHash.add(copyList);
			}
		}
	}
	
}
