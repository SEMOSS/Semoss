package prerna.algorithm.learning.util;

public interface ICategoricalCluster {

	/**
	 * Update when adding to the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
	 * @param attributeInstance				The name of the instance
	 * @param value							The value of the instance
	 */
	void addToCluster(String attributeName, String attributeInstance, Double value);
	
	/**
	 * Update when removing from the cluster for the given attribute and value
	 * @param attributeName					The name of the attribute
 	 * @param attributeInstance				The name of the instance
	 * @param value							The value of the attribute
	 */
	void removeFromCluster(String attributeName, String attributeInstance, Double value);
	
	/**
	 * Determine if the cluster is empty
	 * @return								boolean true if object is empty, false otherwise
	 */
	boolean isEmpty();
}
