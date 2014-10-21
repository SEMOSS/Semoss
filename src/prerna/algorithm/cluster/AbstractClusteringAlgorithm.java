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
	protected ClusteringNumericalMethods cnm;
	
	//the index of each instance and the total number of instances
	protected Hashtable<String,Integer> instanceIndexHash;
	protected int numInstances;

	// the total number of clusters, the number of instances in each cluster, and the cluster each instance is assigned to
	protected int numClusters;
	protected int[] numInstancesInCluster;
	protected int[] clustersAssigned;

	//the category and number values for each instance
	protected String[][] instanceCategoryMatrix;
	protected String[][] instanceNumberBinMatrix;
	protected String[][] instanceNumberBinOrderingMatrix;
//	protected Double[][] instanceNumberMatrix;

	//the category and number values for each cluster
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix;
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix;
//	protected Double[][] clusterNumberMatrix;
		
	//rows for the summarizing cluster
	protected ArrayList<Object[]> clusterSummaryRows;

	//indexing used for visualization
	protected int[] numericalPropIndices;
	protected Integer[] categoryPropIndices; 

	//success of algorithm
	protected boolean success;
	
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
	
	public int[] getNumInstancesInCluster() {
		return numInstancesInCluster;
	}

	public Hashtable<String,Integer> getInstanceIndexHash() {
		return instanceIndexHash;
	}

	public ArrayList<Object[]> getSummaryClusterRows() {
		return clusterSummaryRows;
	}

	public AbstractClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
		setDataVariables();
	}
	
	// method to be defined in specific clustering algorithms
	public abstract boolean execute();

	protected void setDataVariables(){
		cdp = new ClusteringDataProcessor(masterTable,varNames);
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
//		instanceNumberMatrix = cdp.getNumericalMatrix();
		instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		instanceNumberBinOrderingMatrix = cdp.getNumericalBinOrderingMatrix();
		instanceIndexHash = cdp.getInstanceHash();
		numInstances = instanceIndexHash.size();
	}
	
	protected void setAlgorithmVariables(){
		cnm = new ClusteringNumericalMethods(instanceNumberBinMatrix, instanceCategoryMatrix, instanceNumberBinOrderingMatrix);
		cnm.setCategoricalWeights(cdp.getCategoricalWeights());
		cnm.setNumericalWeights(cdp.getNumericalWeights());
		
		//create cluster assignment matrix for each instance
		numInstancesInCluster = initalizeClusterMatrix(numClusters);
		//randomly assign one instance to each cluster
		clustersAssigned = randomlyAssignClusters(numInstances, numClusters);
		//make the custer number matrix from initial assignments
//		clusterNumberMatrix = createClustersNumberProperties(instanceNumberMatrix, clustersAssigned, numClusters);
		clusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clustersAssigned, numClusters);
		//make the cluster category matrix from initial assignments
		clusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clustersAssigned, numClusters);
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
		clustersAssigned[0] = 0;
//		Double[][] initialClusterNumberMatrix = createClustersNumberProperties(instanceNumberMatrix, clustersAssigned, 1);
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterNumberMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clustersAssigned, 1);
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clustersAssigned, 1);
	
		for(i = 1; i < numClusters; i++) {
			int j;
			int minIndex = -1; //initialize to impossible value for index
			double minSimilarity = (double) 2; //initialize to value larger than 1 since max value is 1
			for(j = 1; j < numInstances; j++) {
				double similarityClusterVal;
				if(initialClusterCategoryMatrix != null) {
					if(initialClusterNumberMatrix != null) {
						similarityClusterVal = cnm.getSimilarityScore(j, initialClusterNumberMatrix.get(0), initialClusterCategoryMatrix.get(0));
					} else {
						similarityClusterVal = cnm.getSimilarityScore(j, null, initialClusterCategoryMatrix.get(0));
					}
				} else {
					similarityClusterVal = cnm.getSimilarityScore(j, initialClusterNumberMatrix.get(0), null);
				}
				if(similarityClusterVal < minSimilarity && (clustersAssigned[j] == -1) ) {
					minIndex = j;
					minSimilarity =  similarityClusterVal;
				}
			}
//			initialClusterNumberMatrix = updateClustersNumberProperties(minIndex, -1, 0, initialClusterNumberMatrix, new int[]{i});
			initialClusterNumberMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(minIndex, -1, 0, instanceNumberBinMatrix, initialClusterNumberMatrix);
			initialClusterCategoryMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(minIndex, -1, 0, instanceCategoryMatrix, initialClusterCategoryMatrix);
			
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

	public double calculateFinalInstancesToClusterSimilarity() {
		if(success) {
			double sumSimiliarities = 0;
			for(String s : instanceIndexHash.keySet()) {
				int dataIdx = instanceIndexHash.get(s);
				int clusterIdx = clustersAssigned[dataIdx];
				if(clusterCategoryMatrix != null) {
					if(clusterNumberBinMatrix != null) {
						sumSimiliarities += cnm.getSimilarityScore(dataIdx, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
					} else {
						sumSimiliarities += cnm.getSimilarityScore(dataIdx, null, clusterCategoryMatrix.get(clusterIdx));
					}
				} else {
					sumSimiliarities += cnm.getSimilarityScore(dataIdx, clusterNumberBinMatrix.get(clusterIdx), null);
				}
			}
			return sumSimiliarities;
		} else {
			return Double.NaN;
		}
	}
	
	public double calculateFinalTotalClusterToClusterSimilarity() {
		if(success) {
			double sumSimiliarities = 0;
			int i;
			for(i = 0; i < numClusters - 1; i++) {
				int j;
				for(j = i+1; j < numClusters; j++) {
					if(clusterCategoryMatrix != null) {
						if(clusterNumberBinMatrix != null) {
							sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, clusterCategoryMatrix);
						} else {
							sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, null, clusterCategoryMatrix);
						}
					} else {
						sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, null);
					}
				}
			}
			return sumSimiliarities;
		} else {
			return Double.NaN;
		}
	}
	
	/**
	 * Print each cluster with categorical and numerical properties and a list of all instances
	 */
	protected void createClusterSummaryRowsForGrid() {
		clusterSummaryRows = new ArrayList<Object[]>();

		String[] numericalPropNames = cdp.getNumericalPropNames();
		String[] categoryPropNames = cdp.getCategoryPropNames();

		for(int clusterInd = 0;clusterInd<numInstancesInCluster.length;clusterInd++) {
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
							double percent = (1.0*freq)/(1.0*numInstancesInCluster[clusterInd])*100;
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
							double percent = (1.0*freq)/(1.0*numInstancesInCluster[clusterInd])*100;
							DecimalFormat nf = new DecimalFormat("###.##");
							clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
						}
					}
				}
			}
			clusterRow[varNames.length] = clusterInd;
			clusterSummaryRows.add(clusterRow);
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
