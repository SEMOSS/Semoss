package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.error.BadInputException;

/** Generic clustering algorithm to cluster instances based on their categorical and numerical properties.
 * 
 */
public class ClusteringAlgorithm {

	// instance variables that must be defined for clustering to work
	private ArrayList<Object[]> masterTable;
	private String[] varNames;
	
	private ClusteringDataProcessor cdp;

	//the index of each instance and the total number of instances
	private Hashtable<String,Integer> instanceIndexHash;
	private int numInstances;
	
	// the total number of clusters, the number of instances in each cluster, and the cluster each instance is assinged to
	private int numClusters;
	private int[] clustersNumInstances;
	private ArrayList<Integer> clustersAssigned;
	
	//the category and number values for each instance
	private String[][] instanceCategoryMatrix;
	private double[][] instanceNumberMatrix;
	
	//the category and number values for each cluster
	private ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix;
	private double[][] clusterNumberMatrix;
	
	//determines whether the algorithm completed successfully.
	private boolean success = false;

	public ClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
	}
	
	public Boolean isSuccessful() {
		return success;
	}
	
	public void setNumClusters(int numClusters) {
		this.numClusters = numClusters;
	}
	
	
	/** Performs the clustering based off of the instance's categorical and numerical properties.
	 * These properties are pulled from the instanceCategoryMatrix and instanceNumberMatrix, that are filled prior to start.
	 * The final cluster each instance is assigned to is stored in clustersAssigned.
	 * The categorical and numerical properties for each cluster based on the instances it contains are stored in clusterCategoryMatrix and clusterNumberMatrix.
	 * The number of instances in each cluster is stored in clustersNumInstances.
	 */
	public void execute() throws BadInputException {
		
		cdp = new ClusteringDataProcessor(masterTable,varNames);
		
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
		instanceNumberMatrix = cdp.getNumericalMatrix();
		instanceIndexHash = cdp.getInstanceHash();
		numInstances = instanceIndexHash.size();
		
		//randomly assign one instance to each cluster
		clustersAssigned = randomlyAssignClusters();
		//make the custer number matrix from initial assignments
		clusterNumberMatrix = createClustersNumberProperties();
		//make the cluster category matrix from initial assingments
		clusterCategoryMatrix = createClustersCategoryProperties();
		
		
		
		boolean noChange = false;
		int iterationCount = 0;
		int maxIterations = 1000;
		//continue until there are no changes, so when noChange == true, quit.
		//or quit after some ridiculously large number of times with an error
		while(!noChange&&iterationCount<maxIterations) {
			noChange = true;
			
			for(String instance : instanceIndexHash.keySet()) {
				int instanceInd = instanceIndexHash.get(instance);
				int newClusterForInstance = findNewClusterForInstance(instanceInd);
				int oldClusterForInstance = clustersAssigned.get(instanceInd);
				if(newClusterForInstance!=oldClusterForInstance) {
					noChange = false;
					clusterNumberMatrix = updateClustersNumberProperties(instanceInd, oldClusterForInstance,newClusterForInstance, clusterNumberMatrix, clustersNumInstances);
					clusterCategoryMatrix = updateClustersCategoryProperties(instanceInd, oldClusterForInstance,newClusterForInstance, clusterCategoryMatrix);
					if(oldClusterForInstance>-1)
						clustersNumInstances[oldClusterForInstance]--;
					clustersNumInstances[newClusterForInstance]++;
					clustersAssigned.set(instanceInd, newClusterForInstance);
				}
			}			
		}
		//if it quits after the ridiculously large number of times, print out the error
		if(iterationCount==maxIterations) {
			success = false;
			System.out.println("Completed Maximum Number of iterations without finding a solution");
		}
		else
			success = true;
		
		printOutClusterForInstance();
		printClusterNumberMatrix();
		printClusterCategoryMatrix();
	}


	/**
	 * Initializes the cluster assignment matrix.
	 * Assigns one random instance to each cluster.
	 * Sets all other instances to have undefined cluster of 0.
	 * @return ArrayList<Integer> randomly assigned clusters for each instance
	 */
	public ArrayList<Integer> randomlyAssignClusters() {
		//initializes the cluster assignment matrix for each instance
		clustersNumInstances = new int[numClusters];
		for(int i=0;i<numClusters;i++)
			clustersNumInstances[i] = 1;
		ArrayList<Integer> clustersAssigned = new ArrayList<Integer>();
		for(int i=0;i<numInstances;i++)
			clustersAssigned.add(-1);
		//assigns one random instance to each cluster to start
		int count = 0;
		while(count<numClusters) {
			int randomInstance = (int)(Math.random() *numInstances);
			if(clustersAssigned.get(randomInstance)==-1) {
				clustersAssigned.set(randomInstance,count);
				count++;
			}
		}
		return clustersAssigned;
	}


	/**
	 * Given a specific instance, find the cluster it is most similar to.
	 * For every cluster, call the similarity function between the system and that cluster.
	 * Compare the similarity score of all the clusters and return the one with max similarity.
	 */
	public int findNewClusterForInstance(int instanceInd) throws BadInputException {
		int clusterIndWithMaxSimilarity = 0;
		double maxSimilarity = cdp.getSimilarityScore(instanceInd,0,clusterNumberMatrix,clusterCategoryMatrix.get(0));
		for(int clusterInd = 0;clusterInd <numClusters;clusterInd++) {
			double similarityForCluster = cdp.getSimilarityScore(instanceInd,clusterInd,clusterNumberMatrix,clusterCategoryMatrix.get(clusterInd));
			if(similarityForCluster>maxSimilarity) {
				maxSimilarity = similarityForCluster;
				clusterIndWithMaxSimilarity = clusterInd;
			}
		}
		return clusterIndWithMaxSimilarity;
	}

	
	/** Creates the initial cluster number property matrix.
	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
	**/
	public double[][] createClustersNumberProperties() {
		
		double[][] clusterNumberMatrix = new double[numClusters][instanceNumberMatrix[0].length];
		for(int clusterInd=0;clusterInd<numClusters;clusterInd++) {
			for(int numberInd=0;numberInd<instanceNumberMatrix[0].length;numberInd++) {
				clusterNumberMatrix[clusterInd][numberInd] = 0.0;
			}
		}
		//iterate through every instance
		for(int instanceInd=0;instanceInd<clustersAssigned.size();instanceInd++) {
			int clusterInd = clustersAssigned.get(instanceInd);
			//if the instance is assigned to a cluster, then put its numerical properties in the cluster Properties Matrix
			if(clusterInd>-1) {
				for(int numberInd = 0;numberInd<instanceNumberMatrix[instanceInd].length;numberInd++) {
					double numberValForInstance = instanceNumberMatrix[instanceInd][numberInd];
					clusterNumberMatrix[clusterInd][numberInd] = numberValForInstance;
				}
			}
		}
		return clusterNumberMatrix;
	}
	
	
	/** Updates the cluster number properties matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	**/
	public double[][] updateClustersNumberProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, double[][] clusterNumberMatrix, int[] clustersNumInstances ) {
		
		//iterate through every numerical property of instance
		//remove from the old cluster index using old val (avg) * oldNum in cluster
		for(int numberInd = 0;numberInd<instanceNumberMatrix[instanceInd].length;numberInd++) {
			double numberValForInstance = instanceNumberMatrix[instanceInd][numberInd];
			
			if(oldClusterForInstance>-1) {
				double oldNumberValForInstance = clusterNumberMatrix[oldClusterForInstance][numberInd];
				double valToPut = (oldNumberValForInstance * clustersNumInstances[oldClusterForInstance] - numberValForInstance) /  (clustersNumInstances[oldClusterForInstance] - 1);
				clusterNumberMatrix[oldClusterForInstance][numberInd] = valToPut;
			}
			
			double newClusterValForInstance = clusterNumberMatrix[newClusterForInstance][numberInd];
			double valToPut = (newClusterValForInstance * clustersNumInstances[newClusterForInstance] + numberValForInstance) /  (clustersNumInstances[newClusterForInstance] + 1);
			clusterNumberMatrix[newClusterForInstance][numberInd] = valToPut;
		}
		return clusterNumberMatrix;
	}

	/** Creates the initial cluster category property matrix.
	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
	**/
	public ArrayList<ArrayList<Hashtable<String,Integer>>> createClustersCategoryProperties() {
		//iterate through every category property of instance and remove it from the old cluster and put it in the new cluster
		ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix = new ArrayList<ArrayList<Hashtable<String,Integer>>>();
		for(int clusterInd=0;clusterInd<numClusters;clusterInd++) {
			ArrayList<Hashtable<String,Integer>> listForCluster = new ArrayList<Hashtable<String,Integer>>();
			for(int categoryInd=0;categoryInd<instanceCategoryMatrix[0].length;categoryInd++) {
				Hashtable<String,Integer> propHash = new Hashtable<String,Integer>();
				listForCluster.add(propHash);
			}
			clusterCategoryMatrix.add(listForCluster);
		}
		
		//iterate through every instance
		for(int instanceInd=0;instanceInd<clustersAssigned.size();instanceInd++) {
			int clusterInd = clustersAssigned.get(instanceInd);
			//if the instance is assigned to a cluster, then put its categorical properties in the cluster category properties Matrix
			if(clusterInd>-1) {
		
				for(int categoryInd=0;categoryInd<instanceCategoryMatrix[instanceInd].length;categoryInd++) {
					String categoryValForInstance = instanceCategoryMatrix[instanceInd][categoryInd];
					
					//add the category properties to the new cluster
					Hashtable<String,Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryInd);
					propValHash.put(categoryValForInstance, 1);
					clusterCategoryMatrix.get(clusterInd).set(categoryInd, propValHash);
				}
			}
		}
		
		return clusterCategoryMatrix;
		
	}
	
	/** Updates the cluster category property matrix for the instance that is switching clusters
	 * This removes the instance's properties from the old clusters properties.
	 * This add the instance's properties to the new cluster's properties.
	**/
	public ArrayList<ArrayList<Hashtable<String,Integer>>> updateClustersCategoryProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix) {
		
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
	
	/**
	 * Prints the cluster each instance is assigned to.
	 */
	public void printOutClusterForInstance() {
		System.out.println("Cluster for each index:");
		for(String instance : instanceIndexHash.keySet()) {
			System.out.print(instance+": "+clustersAssigned.get(instanceIndexHash.get(instance))+", ");
		}
	}

	public void printClusterNumberMatrix() {
		System.out.println("Cluster Numerical Properties:");

		ArrayList<String> numericalPropNames = cdp.getNumericalPropNames();
		for(int clusterInd=0;clusterInd<clusterNumberMatrix.length;clusterInd++ ) {
			System.out.println("Cluster " + clusterInd + " Numerical Properties: ");
			for(int numberInd=0;numberInd<clusterNumberMatrix[0].length;numberInd++ ) {
				System.out.print(numericalPropNames.get(numberInd) +": "+ clusterNumberMatrix[clusterInd][numberInd]+", ");				
			}
			System.out.println("\n");
		}
	}
	
	public void printClusterCategoryMatrix() {
		System.out.println("\nCluster Category Properties:");

		ArrayList<String> categoryPropNames = cdp.getCategoryPropNames();
		for(int clusterInd=0;clusterInd<clusterCategoryMatrix.size();clusterInd++ ) {
			System.out.println("Cluster " + clusterInd + " Categorical Properties: ");
			for(int numberInd=0;numberInd<clusterCategoryMatrix.get(0).size();numberInd++ ) {
				System.out.print(categoryPropNames.get(numberInd)+": {");
				Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(numberInd);
				for(String propVal : propValHash.keySet()) {
					System.out.print(propVal+": "+propValHash.get(propVal)+"; ");
				}		
				System.out.println("},");
			}
			System.out.println();
		}
	}


	
}
