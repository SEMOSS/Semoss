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
	
	public ArrayList<Object[]> getMasterTable() {
		return masterTable;
	}
	
	public String[] getNames() {
		return masterNames;
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
		inm = new InstanceNumericalMethods(cdp.getNumericalBinMatrix(), cdp.getCategoricalMatrix(), cdp.getNumericalBinOrderingMatrix());
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
		
		// print out similarity matrix for debugging
//		System.out.println("SIMILARITY MATRIX");
//		for(i = 0; i < numInstances; i++) {
//			for(j = 0; j < numInstances; j++) {
//				System.out.print(similarityMatrix[i][j] + ", ");
//			}
//			System.out.println();
//		}
	}
	
	private void calculateKSimilarityMatrix() {
		kSimilarityArr = new double[numInstances];
		kSimilarityIndicesMatrix = new int[numInstances][];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			double[] similarityArrBetweenInstanceToAllOtherInstances = similarityMatrix[i];
			int[] kNearestNeighbors = inm.kNearestNeighbors(similarityArrBetweenInstanceToAllOtherInstances.clone(), i, k);
			// kNearestNeighbors has the most similar similar index first and least similar index last
			if(kNearestNeighbors != null && kNearestNeighbors.length != 0) {
				int mostDistantNeighborIndex = kNearestNeighbors[kNearestNeighbors.length - 1];
				kSimilarityArr[i] = similarityArrBetweenInstanceToAllOtherInstances[mostDistantNeighborIndex];
				kSimilarityIndicesMatrix[i] = kNearestNeighbors;
			} else {
				kSimilarityArr[i] = 0;
			}
		}
		
		// print out k neighborhood matrix for debugging
//		System.out.println("K-NEIGHBORHOOD SIM MATRIX");
//		for(i = 0; i < numInstances; i++) {
//			int[] simIndicies = kSimilarityIndicesMatrix[i];
//			if(simIndicies != null) {
//				for(int j = 0; j < simIndicies.length - 1; j++) {
//					int col = simIndicies[j];
//					System.out.print(similarityMatrix[i][col] + ", ");
//				}
//			}
//			System.out.println();
//		}
		
		// print out k neighborhood matrix for debugging
//		System.out.println("K-NEIGHBORHOOD MATRIX");
//		for(i = 0; i < numInstances; i++) {
//			int[] simIndicies = kSimilarityIndicesMatrix[i];
//			if(simIndicies != null) {
//				for(int j = 0; j < simIndicies.length - 1; j++) {
//					System.out.print(simIndicies[j] + ", ");
//				}
//			}
//			System.out.println();
//		}
	}

	private void calculateReachSimilarity() {
		reachSimMatrix = new double[numInstances][numInstances];
		
		int i;
		int j;
		int counter = 0;
		for(i = 0; i < numInstances; i++) {
			for(j = numInstances - 1; j >= 0 + counter; j--) {
				// reach similarity is the minimum of the similarity between object i and j and the min similarity of the k closest clusters
				if(kSimilarityArr[i] < .1 || kSimilarityArr[j] < .1) {
					reachSimMatrix[i][j] = 0;
					reachSimMatrix[j][i] = 0;
				} else {
					double val = Math.min(similarityMatrix[i][j], kSimilarityArr[i]);
					reachSimMatrix[i][j] = val;
					reachSimMatrix[j][i] = val;
				}
			}
			counter++;
		}
		
		// print out reach-similarity matrix for debugging
//		System.out.println("REACH SIMILARITY MATRIX");
//		for(i = 0; i < numInstances; i++) {
//			for(j = 0;  j < numInstances; j++) {
//				System.out.print(reachSimMatrix[i][j] + ", ");
//			}
//			System.out.println();
//		}
	}
	
	private void calculateLRD() {
		lrd = new double[numInstances];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			double sumReachSim = 0;
			if(kClosestNeighbors != null) {
				for(int j : kClosestNeighbors) {
					sumReachSim += reachSimMatrix[i][j];
				}
				lrd[i] = sumReachSim/kClosestNeighbors.length;
			} else {
				lrd[i] = 0;
			}
		}
	}

	private void calculateLOF() {
		lof = new double[numInstances];
		
		int i;
		for(i = 0; i < numInstances; i++) {
			double sumLRD = 0;
			double sumReachSim = 0;
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			if(kClosestNeighbors != null) {
				for(int j : kClosestNeighbors) {
					sumLRD += lrd[j];
					sumReachSim += reachSimMatrix[i][j];
				}
				lof[i] = sumLRD / sumReachSim;
			} else {
				lof[i] = Double.POSITIVE_INFINITY;
			}
		}
	}
	
	private void calculateZScore() {
		zScore = StatisticsUtilityMethods.calculateZScoresIgnoringInfinity(lof, false);
	}
}
