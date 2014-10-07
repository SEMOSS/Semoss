package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.math.StatisticsUtilityMethods;

/**
 * Algorithm logic:
 * 1) Separate instances into separate clusters based on one specific property(column)
 * 2) Calculate the centers for each cluster (excluding the column used to separate them originally)
 * 3) Determine for each instance, which cluster it is most similar to based on all properties (except the one used to separate them originally)
 * 4) Keep track of where the instance is closest to and if it is the same cluster as when it was originally separated
 * 
 */
public class ClusteringClassification {

	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	private int[][] originalClusterAssignedIndices;
	private int[][] newClusterAssignedIndices;
	private int[][][] countMatrix;
	
	private double[] accuracy;
	private double[] precision;

	public double[] getAccuracy() {
		return accuracy.clone();
	}

	public double[] getPrecision() {
		return precision.clone();
	}

	public ClusteringClassification(ArrayList<Object[]> masterTable, String[] masterNames) {
		ClusterRemoveDuplicates formatter = new ClusterRemoveDuplicates(masterTable, masterNames);
		this.masterTable = formatter.getRetMasterTable();
		this.masterNames = formatter.getRetVarNames();
	}
	
	public void execute() {
		int i;
		int numCols = masterTable.get(0).length;
		int numRows = masterTable.size();
		
		countMatrix = new int[numCols - 1][][];
		originalClusterAssignedIndices = new int[numCols][numRows];
		newClusterAssignedIndices = new int[numCols][numRows];

		for(i = 1; i < numCols; i++) {
			ArrayList<Object[]> clusterData = useColumnFromList(masterTable, i);
			String[] clusterNames = useNameFromList(masterNames, i);
			
			ClusteringOptimization alg = new ClusteringOptimization(clusterData, clusterNames);
			alg.determineOptimalCluster();
			alg.execute();
			
			originalClusterAssignedIndices[i] = alg.getClustersAssigned();
			Hashtable<String, Integer> instanceIndexHash = alg.getInstanceIndexHash();
			int[] numInstancesInCluster = alg.getNumInstancesInCluster();
			
			ArrayList<Object[]> data = removeColumnFromList(masterTable, i);
			String[] names = removeNameFromList(masterNames, i);
			// process through data
			ClusteringDataProcessor cdp = new ClusteringDataProcessor(data, names);
			// take results of data processing to generate cluster centers
			ClusteringNumericalMethods cnm = new ClusteringNumericalMethods(cdp.getNumericalBinMatrix(), cdp.getCategoricalMatrix());
			cnm.setCategoricalWeights(cdp.getCategoricalWeights());
			cnm.setNumericalWeights(cdp.getNumericalWeights());
			
			// generate cluster centers
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix = cnm.generateCategoricalClusterCenter(originalClusterAssignedIndices[i], instanceIndexHash);
			ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix = cnm.generateNumericClusterCenter(originalClusterAssignedIndices[i], instanceIndexHash);
			
			// determine where each instance is closest to but never change the state
			newClusterAssignedIndices[i] = new int[originalClusterAssignedIndices[i].length];
			for(String instance : instanceIndexHash.keySet()) {
				int instanceInd = instanceIndexHash.get(instance);
				int mostSimilarCluster = ClusterUtilityMethods.findNewClusterForInstance(cnm, clusterCategoryMatrix, clusterNumberBinMatrix, numInstancesInCluster, instanceInd);
				newClusterAssignedIndices[i][instanceInd] = mostSimilarCluster;
			}
			calculateMatrix(originalClusterAssignedIndices[i], newClusterAssignedIndices[i], i-1);
		}
		
		double[] accuracy = calculateAccuracy();
		double[] precision = calculatePercision();
		
		for(i = 0; i < accuracy.length; i++) {
			System.out.println("Accuracy for " + masterNames[i+1] + ": "
					+ String.format("%.2f%%", accuracy[i])
					+ "\n---------------------------------");
			System.out.println("Percision for " + masterNames[i+1] + ": "
					+ String.format("%.2f", precision[i])
					+ "\n---------------------------------");
		}
	}
	
	
	private double[] calculateAccuracy() {
		accuracy = new double[countMatrix.length];
		int numRows = newClusterAssignedIndices[0].length;

		int i;
		int j;
		for(i = 0; i < countMatrix.length; i++) {
			int[][] innerMatrix = countMatrix[i];
			int sumCorrect = 0;
			for(j = 0; j < innerMatrix.length; j++) {
				sumCorrect += innerMatrix[j][j];
			}
			accuracy[i] = (double) sumCorrect/numRows;
		}
		
		return accuracy;
	}
	
	public double[] calculatePercision() {
		int numRows = newClusterAssignedIndices[0].length;
		precision = new double[countMatrix.length];
		
		double[] pObs;
		if(accuracy == null) {
			pObs = calculateAccuracy();
		} else {
			pObs = accuracy;
		}
		
		double[] pExp = new double[countMatrix.length];
		int i;
		int j;
		int k;
		for(i = 0; i < countMatrix.length; i++) {
			int[][] innerMatrix = countMatrix[i];
			int size = innerMatrix.length;
			
			int[] sumRows = new int[size];
			int[] sumCols = new int[size];
			for(j = 0; j < size; j++) {
				for(k = 0; k < size; k++) {
					sumRows[j] += innerMatrix[k][j];
					sumCols[j] += innerMatrix[j][k];
				}
			}
			
			int total = 0;
			for(j = 0; j < size; j++) {
				total += sumRows[j];
			}
			
			for(j = 0; j < size; j++) {
				pExp[i] += (double) sumRows[j] * sumCols[j] / Math.pow(total,2);
			}
		}
		
		for(i = 0; i < countMatrix.length; i++) {
			precision[i] = ( pObs[i] - pExp[i] ) / (1 - pExp[i]);
		}
		
		return precision;
	}

	private void calculateMatrix(int[] oldClusterAssignedIndices,int[] newClusterAssignedIndices, int col) {
		int numClusters = StatisticsUtilityMethods.getMaximumValue(oldClusterAssignedIndices) + 1;

		int[][] innerMatrix = new int[numClusters][numClusters];
		
		int size = oldClusterAssignedIndices.length;

		int i;
		for(i = 0; i < size; i++) {
			innerMatrix[oldClusterAssignedIndices[i]][newClusterAssignedIndices[i]]++;
		}
		
		countMatrix[col] = innerMatrix;
	}

	private ArrayList<Object[]> useColumnFromList(ArrayList<Object[]> list, int colToUse) {
		int numRows = list.size();
		ArrayList<Object[]> retList = new ArrayList<Object[]>(numRows);
		
		int i;
		for(i = 0; i < numRows; i++) {
			Object[] newRow = new Object[2];
			Object[] oldRow = list.get(i);
			
			newRow[0] = oldRow[0];
			newRow[1] = oldRow[colToUse];
			
			retList.add(newRow);
		}
		
		return retList;
	}
	
	private String[] useNameFromList(String[] name, int colToUse) {
		String[] retNames = new String[2];
		retNames[0] = name[0];
		retNames[1] = name[colToUse];
		
		return retNames;
	}
	
	private ArrayList<Object[]> removeColumnFromList(ArrayList<Object[]> list, int colToRemove) {
		int numRows = list.size();
		int numCols = list.get(0).length;
		ArrayList<Object[]> retList = new ArrayList<Object[]>(numRows);
		
		int i;
		int j;
		for(i = 0; i < numRows; i++) {
			Object[] newRow = new Object[numCols - 1];
			Object[] oldRow = list.get(i);
			int counter = 0;
			for(j = 0; j < numCols; j++) {
				if(j != colToRemove) {
					newRow[counter] = oldRow[j];
					counter++;
				}
			}
			retList.add(newRow);
		}
		
		return retList;
	}
	
	private String[] removeNameFromList(String[] name, int colToRemove) {
		int numCols = name.length;

		String[] retNames = new String[numCols - 1];
		int i;
		int counter = 0;
		for(i = 0; i < numCols; i++) {
			if(i != colToRemove) {
				retNames[counter] = name[i];
				counter++;
			}
		}
		
		return retNames;
	}
	
}
