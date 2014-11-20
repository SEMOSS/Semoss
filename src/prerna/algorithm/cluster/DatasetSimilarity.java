package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;

public class DatasetSimilarity {

	private ClusteringNumericalMethods cnm;
	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	
	private String[][] instanceCategoricalMatrix;
	private String[][] instanceNumbericalBinMatrix;
	
	private int size;
	
	private double[] similarityScoresToCluster;
	
	private int numClusters = 1;
	
	private ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix;
	private ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix;
	
	public DatasetSimilarity(ArrayList<Object[]> list, String[] names) {
		ClusterRemoveDuplicates crd = new ClusterRemoveDuplicates(list, names);
		this.masterTable = crd.getRetMasterTable();
		this.masterNames = crd.getRetVarNames();
		
		ClusteringDataProcessor cdp = new ClusteringDataProcessor(masterTable, masterNames);
		instanceCategoricalMatrix = cdp.getCategoricalMatrix();
		instanceNumbericalBinMatrix = cdp.getNumericalBinMatrix();
		cnm = new ClusteringNumericalMethods(instanceNumbericalBinMatrix, instanceCategoricalMatrix, cdp.getNumericalBinOrderingMatrix());
		cnm.setCategoricalWeights(cdp.getCategoricalWeights());
		cnm.setNumericalWeights(cdp.getNumericalWeights());
		
		size = masterTable.size();
	}
	
	public void generateClusterCenters() {
		
		// clustersAssigned is default 0 for each entry, so each instance is in the same cluster
		int[] clustersAssigned = new int[size];
		
		clusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumbericalBinMatrix, clustersAssigned, numClusters);
		//make the cluster category matrix from initial assignments
		clusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoricalMatrix, clustersAssigned, numClusters);
	}
	
	
	public double[] getSimilarityValuesForInstances() {
		
		similarityScoresToCluster = new double[size];
		//iterate through the instance values to generate the similarityScores for each instance
		int j = 0;
		for(; j < size; j++) {
			if(clusterCategoryMatrix != null) {
				if(clusterNumberBinMatrix != null) {
					similarityScoresToCluster[j] = cnm.getSimilarityScore(j, clusterNumberBinMatrix.get(0), clusterCategoryMatrix.get(0));
				} else {
					similarityScoresToCluster[j] = cnm.getSimilarityScore(j, null, clusterCategoryMatrix.get(0));
				}
			} else {
				similarityScoresToCluster[j] = cnm.getSimilarityScore(j, clusterNumberBinMatrix.get(0), null);
			}
		}
		
		return similarityScoresToCluster;
	}
	
	
	
	
}
