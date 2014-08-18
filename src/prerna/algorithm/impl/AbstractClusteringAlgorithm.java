package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;

public abstract class AbstractClusteringAlgorithm {

	// instance variables that must be defined for clustering to work
	protected ArrayList<Object[]> masterTable;
	protected String[] varNames;

	protected ClusteringDataProcessor cdp;

	//the index of each instance and the total number of instances
	protected Hashtable<String,Integer> instanceIndexHash;
	protected int numInstances;

	// the total number of clusters, the number of instances in each cluster, and the cluster each instance is assigned to
	protected int numClusters;
	protected int[] clustersNumInstances;
	protected int[] clustersAssigned;

	//the category and number values for each instance
	protected String[][] instanceCategoryMatrix;
	protected Double[][] instanceNumberMatrix;

	//the category and number values for each cluster
	protected ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix;
	protected Double[][] clusterNumberMatrix;

	//rows for the matrix depending on cluster
	protected ArrayList<Object[]> clusterRows;

	//indexing used for visualization
	protected int[] numericalPropIndices;
	protected Integer[] categoryPropIndices; 

	public int[] getNumericalPropIndices() {
		return numericalPropIndices;
	}

	public Integer[] getCategoryPropIndices() {
		return categoryPropIndices;
	}

	public void setNumClusters(int numClusters) {
		this.numClusters = numClusters;
	}

	public int[] getClustersAssigned() {
		return clustersAssigned;
	}

	public Hashtable<String,Integer> getInstanceIndexHash() {
		return instanceIndexHash;
	}

	public ArrayList<Object[]> getClusterRows() {
		return clusterRows;
	}

	public AbstractClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
	}

	// method to be defined in specific clustering algorithms
	protected abstract boolean execute();

	protected void setUpAlgorithmVariables(){
		cdp = new ClusteringDataProcessor(masterTable,varNames);
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
		instanceNumberMatrix = cdp.getNumericalMatrix();
		instanceIndexHash = cdp.getInstanceHash();
		numInstances = instanceIndexHash.size();
		//create cluster assignment matrix for each instance
		clustersNumInstances = initalizeClusterMatrix(numClusters);
		//randomly assign one instance to each cluster
		clustersAssigned = randomlyAssignClusters(numInstances, numClusters);
		//make the custer number matrix from initial assignments
		clusterNumberMatrix = createClustersNumberProperties(instanceNumberMatrix, clustersAssigned, numClusters);
		//make the cluster category matrix from initial assignments
		clusterCategoryMatrix = createClustersCategoryProperties(instanceCategoryMatrix, clustersAssigned, numClusters);
	}

	private final int[] initalizeClusterMatrix(int numClusters) {
		//initializes the cluster assignment matrix for each instance
		int[] clustersNumInstances = new int[numClusters];
		int i;
		for(i = 0; i < numClusters; i++) {
			clustersNumInstances[i] = 1;
		}
		return clustersNumInstances;
	}

	private final int[] randomlyAssignClusters(int numInstances, int numClusters) {
		int[] clustersAssigned = new int[numInstances];
		int i;
		for(i = 0; i < numInstances; i++) {
			clustersAssigned[i] = -1;
		}
		//assigns one random instance to each cluster to start
		int count = 0;
		while(count < numClusters) {
			int randomInstance = (int)(Math.random() *numInstances);
			if(clustersAssigned[randomInstance] == -1) {
				clustersAssigned[randomInstance] = count;
				count++;
			}
		}
		return clustersAssigned;
	}

	/** Creates the initial cluster number property matrix.
	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
	 **/
	private final Double[][] createClustersNumberProperties(Double[][] instanceNumberMatrix, int[] clustersAssigned, int numClusters) {
		if(instanceNumberMatrix != null) {
			int numNumericProp = instanceNumberMatrix[0].length;
			Double[][] clusterNumberMatrix = new Double[numClusters][numNumericProp];
			int numInstances = clustersAssigned.length;
			//iterate through every instance
			int instanceIdx;
			for(instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
				int clusterInd = clustersAssigned[instanceIdx];
				//if the instance is assigned to a cluster, then put its numerical properties in the cluster Properties Matrix
				if(clusterInd > -1) {
					int numberInd;
					for(numberInd = 0; numberInd < numNumericProp; numberInd++) {
						Double numberValForInstance = instanceNumberMatrix[instanceIdx][numberInd];
						clusterNumberMatrix[clusterInd][numberInd] = numberValForInstance;
					}
				}
			}
			return clusterNumberMatrix;
		}
		return null;
	}

	/** Creates the initial cluster category property matrix.
	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
	 **/
	private final ArrayList<ArrayList<Hashtable<String,Integer>>> createClustersCategoryProperties(String[][] instanceCategoryMatrix, int[] clustersAssigned, int numClusters) {
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
						propValHash.put(categoryValForInstance, 1);
						clusterCategoryMatrix.get(clusterInd).set(categoryIdx, propValHash);
					}
				}
			}
			return clusterCategoryMatrix;
		}
		return null;
	}
	
	/** Updates the cluster number properties matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	 **/
	protected Double[][] updateClustersNumberProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, Double[][] clusterNumberMatrix, int[] clustersNumInstances ) {
		//iterate through every numerical property of instance
		//remove from the old cluster index using old val (avg) * oldNum in cluster
		if(instanceNumberMatrix != null)
		{
			int numNumericalProps = instanceNumberMatrix[0].length;
			int numberIdx;
			for(numberIdx = 0; numberIdx < numNumericalProps; numberIdx++) {
				double numberValForInstance = instanceNumberMatrix[instanceInd][numberIdx];
				// satisfies condition if the instance is already in a cluster
				if(oldClusterForInstance > -1) {
					// update the old cluster
					double oldNumberValForInstance = clusterNumberMatrix[oldClusterForInstance][numberIdx];
					double valToPut = (oldNumberValForInstance * clustersNumInstances[oldClusterForInstance] - numberValForInstance) /  (clustersNumInstances[oldClusterForInstance] - 1);
					clusterNumberMatrix[oldClusterForInstance][numberIdx] = valToPut;
				}
				// update the new cluster
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
	protected ArrayList<ArrayList<Hashtable<String,Integer>>> updateClustersCategoryProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix) {
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
					propValHash.put(categoryValForInstance,propCount);
					clusterCategoryMatrix.get(oldClusterForInstance).set(categoryInd, propValHash);
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
}
