package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import prerna.math.DistanceCalculator;

public final class ClusterUtilityMethods {

	// static class used to calculate different distance measures for numerical similarity score
	private static DistanceCalculator disCalculator = new DistanceCalculator();
	
	private ClusterUtilityMethods() {
		
	}
	
	/**
	 * Calculates the similarity score for the categorical entries
	 * @param propNum					The number of 
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public static double calculateCategorySimilarity(int propNum, int totalProps, double[] weights, String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double categorySimilarity = 0;
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
			if(instaceCategoricalInfo[i] == null) {
				System.out.println("this error");
			}
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			categorySimilarity += weights[i] * numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalProps;

//		logger.info("Calculated similarity score for categories: " + coeff * categorySimilarity);
		return coeff * categorySimilarity;
	}

	/**
	 * Calculates the similarity score for the numerical entries using entropy
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public static double calcuateNumericalSimilarityUsingEntropy(int propNum, int totalProps, double[] weights, String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double categorySimilarity = 0;
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
			categorySimilarity += weights[i] * numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalProps;

//		logger.info("Calculated similarity score for categories: " + coeff * categorySimilarity);
		return coeff * categorySimilarity;
	}
	
	/**
	 * Calculates the similarity score for the numerical entries using distance
	 * @param clusterIdx					The index of the specific cluster we are looking to add the instance into
	 * @param instanceNumericalInfo			The numerical information for the specific index
	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
	 * @return								The similarity score value associated with the numerical properties
	 * @throws IllegalArgumentException		The number of rows for the two arrays being compared to calculate Euclidian distance are of different length 
	 */
	public static double calcuateNumericalSimilarityUsingDistance(int propNum, int totalProps, int clusterIdx, Double[] instanceNumericalInfo, Double[][] allNumericalClusterInfo) throws IllegalArgumentException 
	{
		double numericalSimilarity = 0;
		double distanceNormalization = 0;
		
		int numClusters = allNumericalClusterInfo.length;
		double[] distance = new double[numClusters];

		// generate array of distances between the instance and the cluster for all numerical properties
		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
			Double[] numericalClusterInfo = allNumericalClusterInfo[i];
			// deal with null values
			// set the values to be the same for this property such that the distance becomes 0
			Double[] copyInstanceNumericalInfo = new Double[instanceNumericalInfo.length];
			for(int j = 0; j < numericalClusterInfo.length; j++) {
				if(numericalClusterInfo[j] == null) {
					copyInstanceNumericalInfo = instanceNumericalInfo;
					if(copyInstanceNumericalInfo[j] == null) {
						numericalClusterInfo[j] = new Double(0);
						copyInstanceNumericalInfo[j] = new Double(0);
					} else {
						numericalClusterInfo[j] = copyInstanceNumericalInfo[j];
					}
				} else if(copyInstanceNumericalInfo[j] == null) {
					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
				} else {
					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
				}
			}
			distance[i] = disCalculator.calculateEuclidianDistance(copyInstanceNumericalInfo, numericalClusterInfo);
			distanceNormalization += distance[i];
		}
		
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalProps;
		
		if(distanceNormalization == 0) {
			return coeff; // values are exactly the same
		}
		
		// normalize all the distances to avoid distortion
		for(int i = 0; i < distance.length; i++) {
			distance[i] /= distanceNormalization;
		}
		
		// distance of instance from cluster is a value between 0 and 1
		double distanceFromCluster = Math.exp(-0.5 * distance[clusterIdx]);
		double sumDistanceFromCluster = 0;
		for(int i = 0; i < distance.length; i++) {
			sumDistanceFromCluster += Math.exp(-0.5 * distance[i]);
		}
		// 
		numericalSimilarity = distanceFromCluster/sumDistanceFromCluster;

//		logger.info("Calculated similarity score for numerical properties: " + coeff * numericalSimilarity);
		return coeff * numericalSimilarity;
	}
	
}
