package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CategoricalCluster extends Hashtable<String, Map<String, Double>> implements ICategoricalCluster {

	private static final Logger LOGGER = LogManager.getLogger(CategoricalCluster.class.getName());
	
	private Map<String, Double> entropyValues;
	private Map<String, Double> weights;
	
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

	@Override
	public void setWeights(Map<String, Double> categoricalWeights) {
		this.weights = categoricalWeights;
	}

	@Override
	public void addToCluster(List<String> attributeNames, List<String> attributeInstances, List<Double> values) {
		
	}

	@Override
	public void removeFromCluster(List<String> attributeNames, List<String> attributeInstances, List<Double> values) {
		
	}

	@Override
	public Double getSimilarity(String attributeName, String attributeInstance) {
		return 0.0;
	}

	@Override
	public Double getSimilarity(List<String> attributeNames, List<String> attributeInstances) {
		return 0.0;
	}

}
