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
	
//	/**
//	 * Get similarity for the instance passed in to this cluster
//	 * @param numericalValues				the values
//	 * @param numericalAttributeValues
//	 */
//	Double getSimilarity(List<String> attributeName, List<Double> value);
	
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

	/*
	 * Resets the cluster
	 */
	void reset();

//	/**
//	 * 
//	 * @param numericalWeights
//	 */
//	void setWeights(Map<String, Double> numericalWeights);
	
}
