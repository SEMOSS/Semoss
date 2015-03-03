package prerna.algorithm.learning.similarity;

import java.util.Map;
import java.util.Set;

import prerna.util.ArrayUtilityMethods;

public class ClusterCenterNumericalMethods extends AbstractNumericalMethods{

	public ClusterCenterNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] instanceNumberBinOrderingMatrix) {
		super(numericalBinMatrix, categoricalMatrix, instanceNumberBinOrderingMatrix);
	}

	/**
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param numericalClusterInfo			Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 */
	public Double getInstanceSimilarityScoreToClusterCenter(
			int dataIdx,
			ClusterCenter numericalClusterInfo, 
			ClusterCenter categoryClusterInfo) 
	{		
		double numericalSimilarity = (double) 0;
		double categorySimilarity = (double) 0;

		if(numericalBinMatrix != null && numericalClusterInfo != null && !numericalClusterInfo.isEmpty()) {
			String[] instanceNumericalInfo = numericalBinMatrix[dataIdx];
			numericalSimilarity = calculateNumericalSimilarityUsingEntropy(numericPropNum, totalPropNum, numericalWeights, instanceNumericalInfo, numericalClusterInfo);
		}

		if(categoricalMatrix != null && categoryClusterInfo != null && !categoryClusterInfo.isEmpty()) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			categorySimilarity = calculateCategoricalSimilarityUsingEntropy(categoricalPropNum, totalPropNum, categoricalWeights, instaceCategoricalInfo, categoryClusterInfo);
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
	public double calculateNumericalSimilarityUsingEntropy(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			String[] instaceNumericalInfo, 
			ClusterCenter numericalClusterInfo) 
	{
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			if(weights[i] == 0) {
				continue;
			}

			String[] sortedBinArr = instanceNumberBinOrderingMatrix[i];
			// check to make sure instances contain value for prop and not all missing data
			if(sortedBinArr != null) {
				// numBins contains the number of bins
				int numBins = sortedBinArr.length;

				double adjustmentFactor = 0;
				Map<String, Double> propertyHash = numericalClusterInfo.get(i);
				Set<String> propKeySet = propertyHash.keySet();
				// sumProperties contains the total number of instances for the property
				int sumProperties = 0;
				for(String propName : propKeySet) {
					sumProperties += propertyHash.get(propName);
				}
				// deal with empty values
				if(instaceNumericalInfo[i].equals("NaN")) {
					if(propKeySet.contains("NaN")) {
						double normalizedNumInBin = (double) propertyHash.get("NaN") / sumProperties;
						adjustmentFactor += normalizedNumInBin;
					} 
					//	else {
					//	// similarity is zero if one value is NaN and the other is not NaN
					//	}
				} else {
					for(String propName : propKeySet) {
						if(!propName.equals("NaN")) {
							double normalizedNumInBin = (double) propertyHash.get(propName) / sumProperties;
							int indexOfInstance = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, instaceNumericalInfo[i]);
							int indexOfPropInCluster = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, propName);
							adjustmentFactor += normalizedNumInBin * calculateAdjustmentFactor(indexOfInstance, indexOfPropInCluster, numBins); 
						}
					}
				}
				similarity += weights[i] * adjustmentFactor;
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		// LOGGER.info("Calculated similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}
	
	/**
	 * Calculates the similarity score for the categorical/numerical-bin entries
	 * @param propNum					The number of categorical/numerical props
	 * @param totalProps				The total number of props used in clustering algorithm
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	public double calculateCategoricalSimilarityUsingEntropy(
			int propNum, 
			int totalPropNum, 
			double[] weights, 
			String[] instaceCategoricalInfo, 
			ClusterCenter categoryClusterInfo) 
	{
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// sumProperties contains the total number of instances for the property
			int sumProperties = 0;
			Map<String, Double> propertyHash = categoryClusterInfo.get(i);
			Set<String> propKeySet = propertyHash.keySet();
			for(String propName : propKeySet) {
				sumProperties += propertyHash.get(propName);
			}
			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			double numOccuranceInCluster = 0;
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			similarity += weights[i] * (double) numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

		//		LOGGER.info("Calculated similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}
	
	/**
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param numericalClusterInfo			Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 */
	public void addToClusterCenter(
			int dataIdx,
			ClusterCenter numericalClusterInfo,
			ClusterCenter categoricalClusterInfo
			)
	{
		addToClusterCenter(dataIdx, numericalClusterInfo, categoricalClusterInfo, 1.0);
	}
	
	/**
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param numericalClusterInfo			Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @param coeff							Factor to multiply by the count for area effects
	 * @return								The similarity score between the instance and the cluster
	 */
	public void addToClusterCenter(
			int dataIdx,
			ClusterCenter numericalClusterInfo,
			ClusterCenter categoricalClusterInfo,
			double coeff
			)
	{
		if(numericalBinMatrix != null) {
			String[] instanceNumericalInfo = numericalBinMatrix[dataIdx];
			int i = 0;
			int numProps = instanceNumericalInfo.length;
			for(; i < numProps; i++) {
				numericalClusterInfo.addToCluster(i, instanceNumericalInfo[i], coeff);
			}
		}

		if(categoricalMatrix != null) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			int i = 0;
			int numProps = instaceCategoricalInfo.length;
			for(; i < numProps; i++) {
				categoricalClusterInfo.addToCluster(i, instaceCategoricalInfo[i], coeff);
			}
		}
	}
	
	/**
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param numericalClusterInfo			Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 */
	public void removeFromClusterCenter(
			int dataIdx,
			ClusterCenter numericalClusterInfo,
			ClusterCenter categoricalClusterInfo
			)
	{
		removeFromClusterCenter(dataIdx, numericalClusterInfo, categoricalClusterInfo, 1.0);
	}
	
	/**
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param numericalClusterInfo			Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
 	 * @param coeff							Factor to multiply by the count for area effects
	 * @return								The similarity score between the instance and the cluster
	 */
	public void removeFromClusterCenter(
			int dataIdx,
			ClusterCenter numericalClusterInfo,
			ClusterCenter categoricalClusterInfo,
			double coeff
			)
	{
		if(numericalBinMatrix != null) {
			String[] instanceNumericalInfo = numericalBinMatrix[dataIdx];
			int i = 0;
			int numProps = instanceNumericalInfo.length;
			for(; i < numProps; i++) {
				numericalClusterInfo.removeFromCluster(i, instanceNumericalInfo[i], coeff);
			}
		}

		if(categoricalMatrix != null) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			int i = 0;
			int numProps = instaceCategoricalInfo.length;
			for(; i < numProps; i++) {
				categoricalClusterInfo.removeFromCluster(i, instaceCategoricalInfo[i], coeff);
			}
		}
	}
}
