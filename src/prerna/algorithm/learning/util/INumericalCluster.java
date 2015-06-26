package prerna.algorithm.learning.util;

public interface INumericalCluster {

	/**
	 * Update when adding to the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
	 * @param value							The value of the attribute
	 */
	void addToCluster(String attributeName, Double value);
	
	/**
	 * Update when removing from the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
	 * @param value							The value of the attribute
	 */
	void removeFromCluster(String attributeName, Double value);
	
	/**
	 * Set the distance measure to use for the attribute
	 * @param attributeName					The name of the attribute
	 * @param distanceMeasure				The distance measure to use
	 */
	void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure);
	
	/**
	 * Determine if the cluster is empty
	 * @return								boolean true if object is empty, false otherwise
	 */
	boolean isEmpty();
}
