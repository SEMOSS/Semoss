package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CategoricalCluster extends Hashtable<String, Map<String, Double>> implements ICategoricalCluster {

	private static final Logger LOGGER = LogManager.getLogger(CategoricalCluster.class.getName());
	
	/**
	 * serialization id
	 */
	private static final long serialVersionUID = -3495117301034986814L;

	/**
	 * Default constructor
	 */
	public CategoricalCluster() {
		
	}
	
	@Override
	public void addToCluster(String attributeName, String attributeInstance, Double value) {
		Map<String, Double> valCount = null;
		
		// old property
		if(this.containsKey(attributeName)) 
		{ 
			valCount = this.get(attributeName);
			if(valCount.containsKey(attributeInstance)) { // old instance value for property
				double currValue = valCount.get(attributeInstance);
				currValue += value;
				valCount.put(attributeInstance, currValue);
			} else { // new instance value for property
				valCount.put(attributeInstance, value);
			}
		} 
		// new property to consider
		else 
		{ 
			valCount = new Hashtable<String, Double>();
			valCount.put(attributeInstance, value);
			this.put(attributeName, valCount);
		} 
	}

	@Override
	public void removeFromCluster(String attributeName, String attributeInstance, Double value) {
		Map<String, Double> valCount = null;
		
		if(this.containsKey(attributeName)) {
			valCount = this.get(attributeName);
			if(valCount.containsKey(attributeInstance)) { // reduce count by value
				double currValue = valCount.get(attributeInstance);
				currValue -= value;
				valCount.put(attributeInstance, currValue);
				if(currValue < 0) {
					LOGGER.error("WARNING!!! Attribute " + attributeName + " with value " + attributeInstance + " is now a negative value...");
				}
			} 
			// instance value cannot be found
			else { 
				throw new NullPointerException("Attribute " + attributeName + " with value " + attributeInstance + " cannot be found in cluster to remove...");
			}
		}
		
		// property not found
		else { 
			throw new NullPointerException("Attribute " + attributeName + " cannot be found in cluster to remove...");
		} 
	}

}
