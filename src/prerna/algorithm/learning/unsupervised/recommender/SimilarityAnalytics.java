package prerna.algorithm.learning.unsupervised.recommender;

import org.apache.commons.math3.linear.BlockRealMatrix;

import prerna.algorithm.api.IAnalyticRoutine;

/**
 * 
 * All algorithms used by the recommender must implement this interface
 *
 */
public interface SimilarityAnalytics extends IAnalyticRoutine{
	
	/**
	 * Perform an algorithm on a matrix. The routine does not necessarily have to 
	 * alter/modify the existing matrix
	 * @param data				The matrix containing the input data for the analytical routine
	 * @return					The resulting vector as a result of the analytical routine
	 */
	public double[] runAlgorithm(BlockRealMatrix data);
	
}
