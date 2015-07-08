package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;

public interface INumericalCluster {

	/**
	 * Update when adding to the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
	 * @param value							The value of the attribute
	 */
	void addToCluster(String attributeName, Double value);
	
	/**
	 * 
	 * @param attributeName
	 * @param value
	 */
	void addToCluster(List<String> attributeName, List<Double> value);
	
	/**
	 * Update when removing from the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
	 * @param value							The value of the attribute
	 */
	void removeFromCluster(String attributeName, Double value);
	
	/**
	 * 
	 * @param attributeName
	 * @param value
	 */
	void removeFromCluster(List<String> attributeName, List<Double> value);
	
	/**
	 * Set the distance measure to use for the attribute
	 * @param attributeName					The name of the attribute
	 * @param distanceMeasure				The distance measure to use
	 */
	void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure);
	
	/**
	 * 
	 * @param attributeName
	 * @param value
	 */
	Double getSimilarity(String attributeName, Double value);
	
	/**
	 * 
	 * @param attributeName
	 * @param value
	 * @param indexToSkip
	 * @return
	 */
	Double getSimilarity(List<String> attributeName, List<Double> value, int indexToSkip);
	
	/**
	 * Determine if the cluster is empty
	 * @return								boolean true if object is empty, false otherwise
	 */
	boolean isEmpty();

	/**
	 * Resets the cluster
	 */
	void reset();

	Map<String, IClusterDistanceMode> getDistanceMeasureForAttribute();

	/**
	 * Get the similarity of one cluster to the other cluster
	 * @param c2							The second cluster to determine how similar it is
	 * @param instanceType					The type of the instance
	 * @return								the similarity value
	 */
	double getClusterSimilarity(INumericalCluster c2, String instanceType);
	
	Map<String, Double> getWeights();
	
}
