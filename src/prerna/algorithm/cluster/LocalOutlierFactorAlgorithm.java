package prerna.algorithm.cluster;

import java.util.ArrayList;

import prerna.math.StatisticsUtilityMethods;

public class LocalOutlierFactorAlgorithm {
	
	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	private int numInstances;
	InstanceNumericalMethods inm;
	
	private double[] lrd;
	private double[] lof;
	private double[] zScore;
	
	private int k;
	
	private double[][] similarityMatrix;
	private double[] kSimilarityArr; 
	private int[][] kSimilarityIndicesMatrix;
	private double[][] reachSimMatrix;
	
	public void setK(int k) {
		this.k = k;
	}
	
	public double[] getLRD() {
		return lrd;
	}
	
	public double[] getLOF() {
		return lof;
	}
	
	public double[] getZScore() {
		return zScore;
	}
	
	public LocalOutlierFactorAlgorithm(ArrayList<Object[]> list, String[] names) {
		ClusterRemoveDuplicates crd = new ClusterRemoveDuplicates(list, names);
		this.masterTable = crd.getRetMasterTable();
		this.masterNames = crd.getRetVarNames();
		
		ClusteringDataProcessor cdp = new ClusteringDataProcessor(masterTable, masterNames);
		inm = new InstanceNumericalMethods(cdp.getNumericalBinMatrix(), cdp.getCategoricalMatrix());
		inm.setCategoricalWeights(cdp.getCategoricalWeights());
		inm.setNumericalWeights(cdp.getNumericalWeights());
		
		numInstances = masterTable.size();
	}
	
	public LocalOutlierFactorAlgorithm(ArrayList<Object[]> masterTable, String[] masterNames, int k) {
		this(masterTable, masterNames);
		this.k = k;
	}
	
	public void execute() {
		calculateSimilarityMatrix();
		calculateKSimilarityMatrix();
		calculateReachSimilarity();
		calculateLRD();
		calculateLOF();
		calculateZScore();
	}

	private void calculateSimilarityMatrix() {
		similarityMatrix = new double[numInstances][numInstances];
		
		int i;
		int j;
		int counter = 0;
		for(i = 0; i < numInstances; i++) {
			for(j = numInstances - 1; j >= 0 + counter; j--) {
				double val = inm.calculateSimilarityBetweenInstances(i, j);
				similarityMatrix[i][j] = val;
				similarityMatrix[j][i] = val;
			}
			counter++;
		}
	}
	
	private void calculateKSimilarityMatrix() {
		kSimilarityArr = new double[numInstances];
		kSimilarityIndicesMatrix = new int[numInstances][];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			double[] similarityArrBetweenInstanceToAllOtherInstances = similarityMatrix[i];
			int[] kNearestNeighbors = inm.kNearestNeighbors(similarityArrBetweenInstanceToAllOtherInstances.clone(), i, k);
			// kNearestNeighbors has the most similar similar index first and least similar index last
			int mostDistantNeighborIndex = kNearestNeighbors[kNearestNeighbors.length - 1];
			kSimilarityArr[i] = similarityArrBetweenInstanceToAllOtherInstances[mostDistantNeighborIndex];
			kSimilarityIndicesMatrix[i] = kNearestNeighbors;
		}
	}

	private void calculateReachSimilarity() {
		reachSimMatrix = new double[numInstances][numInstances];
		
		int i;
		int j;
		int counter = 0;
		for(i = 0; i < numInstances; i++) {
			for(j = numInstances - 1; j >= 0 + counter; j--) {
				// reach similarity is the minimum of the similarity between object i and j and the min similarity of the k closest clusters
				double val = Math.min(similarityMatrix[i][j], kSimilarityArr[i]);
				reachSimMatrix[i][j] = val;
				reachSimMatrix[j][i] = val;
			}
			counter++;
		}
	}
	
	private void calculateLRD() {
		lrd = new double[numInstances];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			double sumReachSim = 0;
			for(int j : kClosestNeighbors) {
				sumReachSim += reachSimMatrix[i][j];
			}
			lrd[i] = kClosestNeighbors.length/sumReachSim;
		}
	}

	private void calculateLOF() {
		lof = new double[numInstances];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			double sumLRD = 0;
			double sumReachSim = 0;
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			for(int j : kClosestNeighbors) {
				sumLRD += lrd[j];
				sumReachSim += reachSimMatrix[j][j];
			}
			
			lof[i] = sumLRD / sumReachSim;
		}
	}
	
	private void calculateZScore() {
		zScore = StatisticsUtilityMethods.calculateZScores(lof, false);
	}
}
