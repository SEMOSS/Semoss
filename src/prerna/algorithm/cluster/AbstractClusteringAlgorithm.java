package prerna.algorithm.cluster;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

public abstract class AbstractClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringAlgorithm.class.getName());
	
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
	//TODO: keep numbers
	protected String[][] instanceCategoryMatrix;
//	protected Double[][] instanceNumberMatrix;
	protected String[][] instanceNumberBinMatrix;

	//the category and number values for each cluster
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix;
	//TODO: delete Double[][]
	protected Double[][] clusterNumberMatrix;
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix;
	
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
		setDataVariables();
	}
	
	public ArrayList<Object[]> getMasterTable() {
		return (ArrayList<Object[]>) this.masterTable.clone();
	}
	
	public String[] getVarNames(){
		return this.varNames.clone();
	}

	// method to be defined in specific clustering algorithms
	public abstract boolean execute();

	protected void setDataVariables(){
		cdp = new ClusteringDataProcessor(masterTable,varNames);
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
//		instanceNumberMatrix = cdp.getNumericalMatrix();
		instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		instanceIndexHash = cdp.getInstanceHash();
		numInstances = instanceIndexHash.size();
	}
	
	protected void setAlgorithmVariables(){
		//create cluster assignment matrix for each instance
		clustersNumInstances = initalizeClusterMatrix(numClusters);
		//randomly assign one instance to each cluster
		clustersAssigned = randomlyAssignClusters(numInstances, numClusters);
		//make the custer number matrix from initial assignments
//		clusterNumberMatrix = createClustersNumberProperties(instanceNumberMatrix, clustersAssigned, numClusters);
		clusterNumberBinMatrix = createClustersCategoryProperties(instanceNumberBinMatrix, clustersAssigned, numClusters);
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
//		int randomValue = new Random().nextInt(numInstances+1);
//		clustersAssigned[randomValue] = 0;
		clustersAssigned[0] = 0;
		//TODO delete below
//		Double[][] initialClusterNumberMatrix = createClustersNumberProperties(instanceNumberMatrix, clustersAssigned, 1);
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterNumberMatrix = createClustersCategoryProperties(instanceNumberBinMatrix, clustersAssigned, 1);
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterCategoryMatrix = createClustersCategoryProperties(instanceCategoryMatrix, clustersAssigned, 1);
	
		for(i = 1; i < numClusters; i++) {
			int j;
			int minIndex = -1; //initialize to impossible value for index
			double minSimilarity = (double) 2; //initialize to value larger than 1 since max value is 1
			for(j = 1; j < numInstances; j++) {
				double similarityClusterVal;
				if(initialClusterCategoryMatrix != null) {
					if(initialClusterNumberMatrix != null) {
						similarityClusterVal = cdp.getSimilarityScore(j, 0, initialClusterNumberMatrix.get(0), initialClusterCategoryMatrix.get(0));
					} else {
						similarityClusterVal = cdp.getSimilarityScore(j, 0, null, initialClusterCategoryMatrix.get(0));
					}
				} else {
					similarityClusterVal = cdp.getSimilarityScore(j, 0, initialClusterNumberMatrix.get(0), null);
				}
				if(similarityClusterVal < minSimilarity && (clustersAssigned[j] == -1) ) {
					minIndex = j;
					minSimilarity =  similarityClusterVal;
				}
			}
//			initialClusterNumberMatrix = updateClustersNumberProperties(minIndex, -1, 0, initialClusterNumberMatrix, new int[]{i});
			initialClusterNumberMatrix = updateClustersCategoryProperties(minIndex, -1, 0, instanceNumberBinMatrix, initialClusterNumberMatrix);
			initialClusterCategoryMatrix = updateClustersCategoryProperties(minIndex, -1, 0, instanceCategoryMatrix, initialClusterCategoryMatrix);
			
			clustersAssigned[minIndex] = i;
		}
		
		return clustersAssigned;
	}

//	/** Creates the initial cluster number property matrix.
//	 * This stores the property values for each cluster based on the one instance assigned to that cluster.
//	 **/
//	private final Double[][] createClustersNumberProperties(Double[][] instanceNumberMatrix, int[] clustersAssigned, int numClusters) {
//		if(instanceNumberMatrix != null) {
//			int numNumericProp = instanceNumberMatrix[0].length;
//			Double[][] clusterNumberMatrix = new Double[numClusters][numNumericProp];
//			int numInstances = clustersAssigned.length;
//			//iterate through every instance
//			int instanceIdx;
//			for(instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
//				int clusterInd = clustersAssigned[instanceIdx];
//				//if the instance is assigned to a cluster, then put its numerical properties in the cluster Properties Matrix
//				if(clusterInd > -1) {
//					int numberInd;
//					for(numberInd = 0; numberInd < numNumericProp; numberInd++) {
//						Double numberValForInstance = instanceNumberMatrix[instanceIdx][numberInd];
//						clusterNumberMatrix[clusterInd][numberInd] = numberValForInstance;
//					}
//				}
//			}
//			return clusterNumberMatrix;
//		}
//		return null;
//	}

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
						if(categoryValForInstance == null) {
							System.out.println("here error");
						}
						propValHash.put(categoryValForInstance, 1);
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
	protected ArrayList<ArrayList<Hashtable<String,Integer>>> updateClustersCategoryProperties(int instanceInd,int oldClusterForInstance,int newClusterForInstance, String[][] instanceData, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix) {
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
	
	
	public double calculateFinalInstancesToClusterSimilarity() {
		double sumSimiliarities = 0;
		for(String s : instanceIndexHash.keySet()) {
			int dataIdx = instanceIndexHash.get(s);
			int clusterIdx = clustersAssigned[dataIdx];
			if(clusterCategoryMatrix != null) {
				if(clusterNumberBinMatrix != null) {
					sumSimiliarities += cdp.getSimilarityScore(dataIdx, clusterIdx, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
				} else {
					sumSimiliarities += cdp.getSimilarityScore(dataIdx, clusterIdx, null, clusterCategoryMatrix.get(clusterIdx));
				}
			} else {
				sumSimiliarities += cdp.getSimilarityScore(dataIdx, clusterIdx, clusterNumberBinMatrix.get(clusterIdx), null);
			}
		}
		return sumSimiliarities;
	}
	
//	protected double calculateFinalInstancesToClusterSimilarity() {
//		double sumSimiliarities = 0;
//		for(String s : instanceIndexHash.keySet()) {
//			int dataIdx = instanceIndexHash.get(s);
//			int clusterIdx = clustersAssigned[dataIdx];
//			sumSimiliarities += cdp.getSimilarityScore(dataIdx, clusterIdx, clusterNumberMatrix, clusterCategoryMatrix.get(clusterIdx));
//		}
//		return sumSimiliarities;
//	}
	
	public double calculateFinalTotalClusterToClusterSimilarity() {
		double sumSimiliarities = 0;
		
		int i;
		for(i = 0; i < numClusters - 1; i++) {
			int j;
			for(j = i+1; j < numClusters; j++) {
				if(clusterCategoryMatrix != null) {
					if(clusterNumberBinMatrix != null) {
						sumSimiliarities += cdp.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, clusterCategoryMatrix);
					} else {
						sumSimiliarities += cdp.calculateClusterToClusterSimilarity(i, j, null, clusterCategoryMatrix);
					}
				} else {
					sumSimiliarities += cdp.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, null);
				}
			}
		}
		
		return sumSimiliarities;
	}
	
//	protected double calculateFinalTotalClusterToClusterSimilarity() {
//		double sumSimiliarities = 0;
//		
//		int i;
//		for(i = 0; i < numClusters - 1; i++) {
//			int j;
//			for(j = i+1; j < numClusters; j++) {
//				sumSimiliarities += cdp.calculateClusterToClusterSimilarity(i, j, clusterNumberMatrix, clusterCategoryMatrix);
//			}
//		}
//		
//		return sumSimiliarities;
//	}
	
	//Currently not using method - decided to use total Cluster to Cluster Similarity
//	public double calculateClusterToClusterSmallestSimPath() {
//		double sumSimiliarities = 0;
//		
//		int i;
//		for(i = 0; i < numClusters - 1; i++) {
//			int j;
//			double maxScore = 0;
//			double simScore = 0;
//			for(j = i+1; j < numClusters; j++) {
//				simScore = cdp.calculateClusterToClusterSimilarity(i, j, clusterNumberMatrix, clusterCategoryMatrix);
//				if(simScore > maxScore) {
//					maxScore = simScore;
//				}
//			}
//			sumSimiliarities += maxScore;
//		}
//		
//		return sumSimiliarities;
//	}
	
	
	
	
	
	/**
	 * Print each cluster with categorical and numerical properties and a list of all instances
	 */
	protected void createClusterRowsForGrid() {
		clusterRows = new ArrayList<Object[]>();

		String[] numericalPropNames = cdp.getNumericalPropNames();
		String[] categoryPropNames = cdp.getCategoryPropNames();

		for(int clusterInd = 0;clusterInd<clustersNumInstances.length;clusterInd++) {
			Object[] clusterRow = new Object[varNames.length+1];
			clusterRow[0] = "";

			for(int propInd=1;propInd<varNames.length;propInd++) {
				String prop = varNames[propInd];
				if(categoryPropNames != null) {
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
					}
				}
				
				if(numericalPropNames != null) {
					int numberInd = ArrayUtilityMethods.calculateIndexOfArray(numericalPropNames, prop);
					if(numberInd >-1) {
						Hashtable<String, Integer> propValHash = clusterNumberBinMatrix.get(clusterInd).get(numberInd);
						String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
						if(!propWithHighFreq.equals("")) {
							int freq = propValHash.get(propWithHighFreq);
							double percent = (1.0*freq)/(1.0*clustersNumInstances[clusterInd])*100;
							DecimalFormat nf = new DecimalFormat("###.##");
							clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
						}
					}
				}
			}
			clusterRow[varNames.length] = clusterInd;
			clusterRows.add(clusterRow);
		}		
	}
	
//	/**
//	 * Print each cluster with categorical and numerical properties and a list of all instances
//	 */
//	protected void createClusterRowsForGrid() {
//		clusterRows = new ArrayList<Object[]>();
//
//		String[] numericalPropNames = cdp.getNumericalPropNames();
//		String[] categoryPropNames = cdp.getCategoryPropNames();
//
//		for(int clusterInd = 0;clusterInd<clustersNumInstances.length;clusterInd++) {
//			Object[] clusterRow = new Object[varNames.length+1];
//			clusterRow[0] = "";
//
//			for(int propInd=1;propInd<varNames.length;propInd++) {
//				String prop = varNames[propInd];
//				if(categoryPropNames != null)
//				{
//					int categoryInd = ArrayUtilityMethods.calculateIndexOfArray(categoryPropNames, prop);
//					if(categoryInd >-1) {
//						Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryInd);
//						String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
//						if(!propWithHighFreq.equals("")) {
//							int freq = propValHash.get(propWithHighFreq);
//							double percent = (1.0*freq)/(1.0*clustersNumInstances[clusterInd])*100;
//							DecimalFormat nf = new DecimalFormat("###.##");
//							clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
//						}
//					}
//				} else {
//					if(numericalPropNames != null) 
//					{
//						int numberInd = ArrayUtilityMethods.calculateIndexOfArray(numericalPropNames, prop);
//						if(numberInd > -1) {
//							clusterRow[propInd] = clusterNumberMatrix[clusterInd][numberInd];
//						} else {
//							LOGGER.error("No properties matched for " + prop);
//						}
//					}
//				}
//			}
//			clusterRow[varNames.length] = clusterInd;
//			clusterRows.add(clusterRow);
//		}		
//	}

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
	
//	/**
//	 * Print each cluster with categorical and numerical properties and a list of all instances
//	 */
//	protected void printOutClusters() {
//		LOGGER.info("Cluster Results-");
//
//		String[] numericalPropNames = cdp.getNumericalPropNames();
//		String[] categoryPropNames = cdp.getCategoryPropNames();
//
//		for(int clusterInd = 0;clusterInd<clustersNumInstances.length;clusterInd++) {
//			LOGGER.info("\n\nCluster "+clusterInd+":");
//
//			//print numerical props
//			LOGGER.info("\nNumerical Properties- ");
//			if(clusterNumberMatrix != null) {
//				for(int numberInd=0;numberInd<clusterNumberMatrix[0].length;numberInd++ ) {
//					LOGGER.info(numericalPropNames[numberInd] +": "+ clusterNumberMatrix[clusterInd][numberInd]+", ");
//				}
//			}
//			//print categorical props
//			LOGGER.info("\nCategorical Properties- ");
//			if(clusterCategoryMatrix != null) {
//				for(int numberInd=0;numberInd<clusterCategoryMatrix.get(0).size();numberInd++ ) {
//					Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(numberInd);
//					String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
//					if(!propWithHighFreq.equals("")) {
//						int freq = propValHash.get(propWithHighFreq);
//						double percent = (1.0*freq)/(1.0*clustersNumInstances[clusterInd])*100;
//						DecimalFormat nf = new DecimalFormat("###.##");
//						LOGGER.info(categoryPropNames[numberInd] +": "+propWithHighFreq+" "+freq+"(frequency) and "+ nf.format(percent)+"%(percentage), ");	
//					}
//				}
//			}
//
//			//print instances
//			LOGGER.info("\nInstances- ");
//			for(String instance : instanceIndexHash.keySet()) {
//				int clusterAssigned = clustersAssigned[instanceIndexHash.get(instance)];
//				if(clusterAssigned == clusterInd)
//					LOGGER.info(instance+", ");
//			}
//		}
//	}
}
