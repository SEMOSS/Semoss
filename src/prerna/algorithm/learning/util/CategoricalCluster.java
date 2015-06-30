package prerna.algorithm.learning.util;

import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CategoricalCluster extends Hashtable<String, Hashtable<String, Double>> implements ICategoricalCluster {

	private static final Logger LOGGER = LogManager.getLogger(CategoricalCluster.class.getName());
	
	private Map<String, Double> weights;
	
	/**
	 * serialization id
	 */
	private static final long serialVersionUID = -3495117301034986814L;

	/**
	 * Default constructor
	 */
	public CategoricalCluster(Map<String, Double> categoricalWeights) {
		weights = categoricalWeights;
	}
	
	@Override
	public void addToCluster(List<String> attributeNames, List<String> attributeInstances, List<Double> values) {
		for(int i = 0; i < attributeNames.size(); i++) {
			this.addToCluster(attributeNames.get(i), attributeInstances.get(i), values.get(i));
		}
	}
	
	@Override
	public void addToCluster(String attributeName, String attributeInstance, Double value) {
		Hashtable<String, Double> valCount = null;
		
		if(this.contains(attributeName)) 
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
	public void removeFromCluster(List<String> attributeNames, List<String> attributeInstances, List<Double> values) {
		for(int i = 0; i < attributeNames.size(); i++) {
			this.removeFromCluster(attributeNames.get(i), attributeInstances.get(i), values.get(i));
		}
	}
	
	@Override
	public void removeFromCluster(String attributeName, String attributeInstance, Double value) {
		Hashtable<String, Double> valCount = null;
		
		if(this.contains(attributeName)) {
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

	public void addWeight(String attributeName, Double weight) {
		weights.put(attributeName, weight);
	}
	
	@Override
	public Double getSimilarity(String attributeName, String attributeInstance) {
		return 0.0;
	}

	@Override
	public Double getSimilarity(List<String> attributeNames, List<String> attributeInstances) {
		double similarity = 0.0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < attributeNames.size(); i++) {
			// sumProperties contains the total number of instances for the property
			double sumProperties = 0;
			Hashtable<String, Double> propertyHash = this.get(attributeNames.get(i));//categoryClusterInfo.get(i);
			Collection<Double> valueCollection = propertyHash.values();
			for(Double val : valueCollection) {
				sumProperties += val;
			}

			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			double numOccuranceInCluster = 0;
			if(propertyHash.contains(attributeInstances.get(i))) {
				numOccuranceInCluster = propertyHash.get(attributeInstances.get(i));
			}
			
			double weight = weights.get(attributeNames.get(i));
			similarity += weight * numOccuranceInCluster / sumProperties;
		}

		return similarity;
	}
}
