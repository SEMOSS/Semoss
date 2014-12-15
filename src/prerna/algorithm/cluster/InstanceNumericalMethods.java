package prerna.algorithm.cluster;

import prerna.util.ArrayUtilityMethods;

public class InstanceNumericalMethods extends AbstractNumericalMethods{

	public InstanceNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] numericalBinOrderingMatrix) {
		super(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
	}

	public double calculateSimilarityBetweenInstances(int instanceIdx1, int instanceIdx2) {
		double categoricalSimilarityScore = 0;
		double numericalSimilarityScore = 0;

		if(categoricalMatrix != null) {
			String[] categoricalValuesForInstance1 = categoricalMatrix[instanceIdx1];
			String[] categoricalValuesForInstance2 = categoricalMatrix[instanceIdx2];
			
			categoricalSimilarityScore = calculateInstanceSimilarity(categoricalValuesForInstance1, categoricalValuesForInstance2, categoricalWeights, categoricalPropNum, totalPropNum);
		}
		
		if(numericalBinMatrix != null) {
			String[] numericalBinValuesForInstance1 = numericalBinMatrix[instanceIdx1];
			String[] numericalBinValuesForInstance2 = numericalBinMatrix[instanceIdx2];
			
			numericalSimilarityScore = calculateNumericalInstanceSimilarity(numericalBinValuesForInstance1, numericalBinValuesForInstance2, numericalWeights, numericPropNum, totalPropNum);
		}	
		
		return categoricalSimilarityScore + numericalSimilarityScore;
	}
	
	public double calculateInstanceSimilarity(String[] categoricalValues1, String[] categoricalValues2, double[] weights, int propNum, int totalPropNum){
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// the values are either the same or different, which results in either adding the weight*1/1 = weight or weight*0/2 = 0
			if(categoricalValues1[i].equals(categoricalValues2[i])) {
				similarity += weights[i];
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

//		LOGGER.info("Calculated instance similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}

	public double calculateNumericalInstanceSimilarity(String[] categoricalValues1, String[] categoricalValues2, double[] weights, int propNum, int totalPropNum){
		double similarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// deal with empty values
			if(categoricalValues1[i].equals("NaN")) {
				if(categoricalValues2[i].equals("NaN")) {
					similarity += weights[i];
				} 
//				else {
//					// similarity is zero if one value is NaN and the other is not NaN
//				}
			} else {
				// the values are either the same or different, which results in either adding the weight*1/1 = weight or weight*0/2 = 0
				String[] sortedBinArr = instanceNumberBinOrderingMatrix[i];
				// numBins contains the number of bins
				int numBins = sortedBinArr.length;
				int indexOfInstance1 = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, categoricalValues1[i]);
				int indexOfInstance2 = ArrayUtilityMethods.calculateIndexOfArray(sortedBinArr, categoricalValues2[i]);
				// adjustment factor simplifies since comparing only one instance to another instance
				double adjustmentFactor = calculateAdjustmentFactor(indexOfInstance1, indexOfInstance2, numBins); 
				similarity += weights[i] * adjustmentFactor;
			}
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * propNum / totalPropNum;

//		LOGGER.info("Calculated instance similarity score for categories: " + coeff * similarity);
		return coeff * similarity;
	}
	
	public int[] kNearestNeighbors(double[] similarityBetweenInstanceToAllOtherInstances, int instanceIdx, int k) {
		int[] kClosestNeighbors = new int[k];
		int numInstances = similarityBetweenInstanceToAllOtherInstances.length;
		int[] indexOrderArr = indexOrder(numInstances);
		
		// loop through and order all values and indices
		int i;
		double tempSimValue;
		int tempIndexValue;
		boolean flag = true;
		while(flag) {
			flag = false;
			for(i = 0; i < numInstances - 1; i++) {
				if(similarityBetweenInstanceToAllOtherInstances[i] < similarityBetweenInstanceToAllOtherInstances[i+1]){
					tempSimValue = similarityBetweenInstanceToAllOtherInstances[i];
					similarityBetweenInstanceToAllOtherInstances[i] = similarityBetweenInstanceToAllOtherInstances[i+1];
					similarityBetweenInstanceToAllOtherInstances[i+1] = tempSimValue;
					// change order of index value
					tempIndexValue = indexOrderArr[i];
					indexOrderArr[i] = indexOrderArr[i+1];
					indexOrderArr[i+1] = tempIndexValue;
					
					flag = true;
				}
			}
		}
		
		int idxCounter = 0;
		int kCounter = 0;
		boolean noKCluster = false;
		while(kCounter < k || idxCounter == numInstances) {
			// don't include the node
			int index = indexOrderArr[idxCounter];
			if(index != instanceIdx) {	
				// don't include nodes with zero similarity
				if(similarityBetweenInstanceToAllOtherInstances[idxCounter] == 0) {
					noKCluster = true;
					return null;
				}
				kClosestNeighbors[kCounter] = index;
				kCounter++;
			}
			idxCounter++;
		}
		
		if(!noKCluster) {
			double lastSimValueInNeighborhood = similarityBetweenInstanceToAllOtherInstances[kCounter - 1];
			for(;idxCounter < numInstances; idxCounter++) {
				int index = indexOrderArr[idxCounter];
				if(index != instanceIdx) {
					double nextSimValueClosestToInstance = similarityBetweenInstanceToAllOtherInstances[idxCounter];
					if(lastSimValueInNeighborhood == nextSimValueClosestToInstance) {
						int newInstanceInNeighborhood =  indexOrderArr[idxCounter];
						try {
							kClosestNeighbors[kCounter] = newInstanceInNeighborhood;
						} catch (IndexOutOfBoundsException e) {
							kClosestNeighbors = ArrayUtilityMethods.resizeArray(kClosestNeighbors, 2);
							kClosestNeighbors[kCounter] = newInstanceInNeighborhood;
						}
						kCounter++;
					} else {
						break;
					}
				}
			}
			
			// to account for case when last index is 0, don't want to remove it from the K-Neighborhood
			if(kClosestNeighbors[kCounter - 1] == 0) {
				kClosestNeighbors[kCounter - 1] = 1;
				kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
				kClosestNeighbors[kCounter - 1] = 0;
			} else {
				kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
			}
		} else {
			kClosestNeighbors = ArrayUtilityMethods.removeAllTrailingZeroValues(kClosestNeighbors);
		}
		
		return kClosestNeighbors;
	}
	
	private int[] indexOrder(int length) {
		int[] retArr = new int[length];
		int i;
		for(i = 0; i < length; i++) {
			retArr[i] = i;
		}
		
		return retArr;
	}
}
