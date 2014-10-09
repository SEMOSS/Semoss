package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import prerna.math.StatisticsUtilityMethods;

public class ClusteringNumericalMethods extends AbstractNumericalMethods{

//	private static final Logger LOGGER = LogManager.getLogger(ClusterUtilityMethods.class.getName());
	
	// static class used to calculate different distance measures for numerical similarity score
//	private static DistanceCalculator disCalculator = new DistanceCalculator();
	
	public ClusteringNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix) {
		super(numericalBinMatrix, categoricalMatrix);
	}
	
	/**
	 * 
	 * @param numericalBinMatrix			All the numeric bin data for every instance
	 * @param categoricalMatrix				All the categorical bin data for every instance
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 */
	public Double getSimilarityScore(
			int dataIdx,
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo, 
			ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{		
		double numericalSimilarity = (double) 0;
		double categorySimilarity = (double) 0;

		if(numericalBinMatrix != null) {
			String[] instanceNumericalInfo = numericalBinMatrix[dataIdx];
			numericalSimilarity = calculateSimilarityUsingEntropy(numericPropNum, totalPropNum, numericalWeights, instanceNumericalInfo, numericalClusterInfo);
		}

		if(categoricalMatrix != null) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			categorySimilarity = calculateSimilarityUsingEntropy(categoricalPropNum, totalPropNum, categoricalWeights, instaceCategoricalInfo, categoryClusterInfo);
		}

		return numericalSimilarity + categorySimilarity;
	}
	
	/**
	 * Calculates the similarity score for the categorical/numerical-bin entries
	 * @param propNum					The number of categorical/numerical props
	 * @param totalProps				The total number of props used in clustering algorithm
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public double calculateSimilarityUsingEntropy(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			String[] instaceCategoricalInfo, 
			ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// sumProperties contains the total number of instances for the property
			double sumProperties = 0;
			Hashtable<String, Integer> propertyHash = categoryClusterInfo.get(i);
			Set<String> propKeySet = propertyHash.keySet();
			for(String propName : propKeySet) {
				sumProperties += propertyHash.get(propName);
			}
			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			int numOccuranceInCluster = 0;
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			similarity += weights[i] * numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

//		LOGGER.info("Calculated similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

//	/**
//	 * Calculates the similarity score for the numerical entries using distance
//	 * @param clusterIdx					The index of the specific cluster we are looking to add the instance into
//	 * @param instanceNumericalInfo			The numerical information for the specific index
//	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
//	 * @return								The similarity score value associated with the numerical properties
//	 * @throws IllegalArgumentException		The number of rows for the two arrays being compared to calculate Euclidian distance are of different length 
//	 */
//	public static double calcuateNumericalSimilarityUsingDistance(int propNum, int totalProps, int clusterIdx, Double[] instanceNumericalInfo, Double[][] allNumericalClusterInfo) throws IllegalArgumentException 
//	{
//		double numericalSimilarity = 0;
//		double distanceNormalization = 0;
//		
//		int numClusters = allNumericalClusterInfo.length;
//		double[] distance = new double[numClusters];
//
//		// generate array of distances between the instance and the cluster for all numerical properties
//		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
//			Double[] numericalClusterInfo = allNumericalClusterInfo[i];
//			// deal with null values
//			// set the values to be the same for this property such that the distance becomes 0
//			Double[] copyInstanceNumericalInfo = new Double[instanceNumericalInfo.length];
//			for(int j = 0; j < numericalClusterInfo.length; j++) {
//				if(numericalClusterInfo[j] == null) {
//					copyInstanceNumericalInfo = instanceNumericalInfo;
//					if(copyInstanceNumericalInfo[j] == null) {
//						numericalClusterInfo[j] = new Double(0);
//						copyInstanceNumericalInfo[j] = new Double(0);
//					} else {
//						numericalClusterInfo[j] = copyInstanceNumericalInfo[j];
//					}
//				} else if(copyInstanceNumericalInfo[j] == null) {
//					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
//				} else {
//					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
//				}
//			}
//			distance[i] = disCalculator.calculateEuclidianDistance(copyInstanceNumericalInfo, numericalClusterInfo);
//			distanceNormalization += distance[i];
//		}
//		
//		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
//		double coeff = 1.0 * propNum / totalProps;
//		
//		if(distanceNormalization == 0) {
//			return coeff; // values are exactly the same
//		}
//		
//		// normalize all the distances to avoid distortion
//		for(int i = 0; i < distance.length; i++) {
//			distance[i] /= distanceNormalization;
//		}
//		
//		// distance of instance from cluster is a value between 0 and 1
//		double distanceFromCluster = Math.exp(-0.5 * distance[clusterIdx]);
//		double sumDistanceFromCluster = 0;
//		for(int i = 0; i < distance.length; i++) {
//			sumDistanceFromCluster += Math.exp(-0.5 * distance[i]);
//		}
//		// 
//		numericalSimilarity = distanceFromCluster/sumDistanceFromCluster;
//
////		LOGGER.info("Calculated similarity score for numerical properties: " + coeff * numericalSimilarity);
//		return coeff * numericalSimilarity;
//	}
	
	
	/**
	 * @param clusterIdx1
	 * @param clusterIdx2
	 * @param clusterNumericalMatrix
	 * @param clusterCategoryMatrix
	 * @return
	 */
	public double calculateClusterToClusterSimilarity(
			int clusterIdx1, 
			int clusterIdx2, 
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterNumericalMatrix, 
			ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix)
	{
		double numericSimilarity = 0;
		double categoricalSimilarity = 0;
		
		if(clusterNumericalMatrix != null) {
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo1 = clusterNumericalMatrix.get(clusterIdx1);
			ArrayList<Hashtable<String, Integer>> numericalClusterInfo2 = clusterNumericalMatrix.get(clusterIdx2);

			numericSimilarity = calculateClusterSimilarity(numericPropNum, totalPropNum, numericalWeights, numericalClusterInfo1, numericalClusterInfo2);
		}

		if(clusterCategoryMatrix != null) {
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo1 = clusterCategoryMatrix.get(clusterIdx1);
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo2 = clusterCategoryMatrix.get(clusterIdx2);

			categoricalSimilarity = calculateClusterSimilarity(categoricalPropNum, totalPropNum, categoricalWeights, categoricalClusterInfo1, categoricalClusterInfo2);
		}

		return numericSimilarity + categoricalSimilarity;
	}
	
	/**
	 * 
	 * @param propNum
	 * @param totalPropNum
	 * @param weights
	 * @param clusterInfo1
	 * @param clusterInfo2
	 * @return 
	 */
	private double calculateClusterSimilarity(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			ArrayList<Hashtable<String, Integer>> clusterInfo1, 
			ArrayList<Hashtable<String, Integer>> clusterInfo2) 
	{
		double coeff = 1.0 * propNum / totalPropNum;
		double similarityScore = 0;

		int i;
		int size = clusterInfo1.size();
		// loop through all properties
		for(i = 0; i < size; i++) {
			// for specific property
			Hashtable<String, Integer> propInfoForCluster1 = clusterInfo1.get(i);
			Hashtable<String, Integer> propInfoForCluster2 = clusterInfo2.get(i);

			int normalizationCount1 = 0;
			for(String propInstance : propInfoForCluster1.keySet()) {
				normalizationCount1 += propInfoForCluster1.get(propInstance);
			}
			int normalizationCount2 = 0;
			for(String propInstance : propInfoForCluster2.keySet()) {
				normalizationCount2 += propInfoForCluster2.get(propInstance);
			}

			int possibleValues = 0;
			double sumClusterDiff = 0;
			for(String propInstance : propInfoForCluster1.keySet()) {
				if(propInfoForCluster2.containsKey(propInstance)) {
					possibleValues++;
					// calculate difference between counts
					int count1 = propInfoForCluster1.get(propInstance);
					int count2 = propInfoForCluster2.get(propInstance);
					sumClusterDiff += Math.abs((double) count1/normalizationCount1 - (double) count2/normalizationCount2);
				} else {
					possibleValues++;
					//include values that 1st cluster has and 2nd cluster doesn't have
					int count1 = propInfoForCluster1.get(propInstance);
					sumClusterDiff += (double) count1/normalizationCount1;
				}
			}
			//now include values that 2nd cluster has that 1st cluster doesn't have
			for(String propInstance: propInfoForCluster2.keySet()) {
				if(!propInfoForCluster1.containsKey(propInstance)) {
					possibleValues++;
					int count2 = propInfoForCluster2.get(propInstance);
					sumClusterDiff += (double) count2/normalizationCount2;
				}
			}

			similarityScore += weights[i] * (1 - sumClusterDiff/possibleValues);
		}

		return coeff * similarityScore;
	}
	
//	/**
//	 * 
//	 * @param clusterIdx1				The index for the cluster we are observing
//	 * @param clusterIdx2				The index for the cluster we are comparing the observed cluster to
//	 * @param clusterNumberMatrix		All the numerical properties
//	 * @param clusterCategoryMatrix		All the categorical properties
//	 * @return
//	 */
//	public double calculateClusterToClusterSimilarity(int clusterIdx1, int clusterIdx2, Double[][] clusterNumberMatrix, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix){
//		double numericSimilarity = 0;
//		double categoricalSimilarity = 0;
//
//		if(clusterNumberMatrix != null) {
//			Double[] numericClusterInfo1 = clusterNumberMatrix[clusterIdx1];
//			numericSimilarity = ClusterUtilityMethods.calcuateNumericalSimilarityUsingDistance(numericProps, totalProps, clusterIdx2, numericClusterInfo1, clusterNumberMatrix);
//		}
//
//		if(clusterCategoryMatrix != null) {
//			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo1 = clusterCategoryMatrix.get(clusterIdx1);
//			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo2 = clusterCategoryMatrix.get(clusterIdx2);
//
//			categoricalSimilarity = calculateClusterCategoricalSimilarity(categoricalClusterInfo1, categoricalClusterInfo2);
//		}
//
//		return numericSimilarity + categoricalSimilarity;
//	}
	
	public ArrayList<ArrayList<Hashtable<String, Integer>>> generateNumericClusterCenter(
			int[] clusterAssigned, 
			Hashtable<String, Integer> instanceIndexHash) 
	{
		int numClusters = StatisticsUtilityMethods.getMaximumValue(clusterAssigned) + 1;
		
		if(numericalBinMatrix != null) {
			ArrayList<ArrayList<Hashtable<String, Integer>>> numericClusterCenter = ClusterUtilityMethods.createClustersCategoryProperties(numericalBinMatrix, clusterAssigned, numClusters);
			return numericClusterCenter;
		}
		
		return null;
	}
	
	public ArrayList<ArrayList<Hashtable<String, Integer>>> generateCategoricalClusterCenter(
			int[] clusterAssigned, 
			Hashtable<String, Integer> instanceIndexHash) 
	{
		int numClusters = StatisticsUtilityMethods.getMaximumValue(clusterAssigned) + 1;
		
		if(categoricalMatrix != null) {
			ArrayList<ArrayList<Hashtable<String, Integer>>> categoricalClusterCenter = ClusterUtilityMethods.createClustersCategoryProperties(categoricalMatrix, clusterAssigned, numClusters);
			return categoricalClusterCenter;
		}
		
		return null;
	}
}
