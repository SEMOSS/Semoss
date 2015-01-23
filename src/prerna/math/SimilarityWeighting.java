package prerna.math;

public final class SimilarityWeighting {

	private SimilarityWeighting() {
		
	}
	
	/**
	 * Generate the weighting values for a list of attributes based on their entropies
	 * @param entropyArr	double[] containing the entropy values for each attribute
	 * @return				double[] containing the weight value for each attribute
	 */
	public static double[] generateWeighting(final double[] entropyArr) {
		int size = entropyArr.length;
		
		double[] weight = new double[size];
		
		double totalEntropy = StatisticsUtilityMethods.getSum(entropyArr);
		int i = 0;
		for(; i < size; i++) {
			if(totalEntropy == 0) {
				weight[i] = 0;
			} else {
				weight[i] = entropyArr[i] / totalEntropy;
			}
		}
		
		return weight;
	}
	
	
}
