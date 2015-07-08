package prerna.algorithm.learning.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CategoricalCluster extends Hashtable<String, Hashtable<String, Double>> implements ICategoricalCluster {

	private static final Logger LOGGER = LogManager.getLogger(CategoricalCluster.class.getName());
	private Map<String, Double> weights = new HashMap<String, Double>();
	
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
	public void removeFromCluster(List<String> attributeNames, List<String> attributeInstances, List<Double> values) {
		for(int i = 0; i < attributeNames.size(); i++) {
			this.removeFromCluster(attributeNames.get(i), attributeInstances.get(i), values.get(i));
		}
	}
	
	@Override
	public void removeFromCluster(String attributeName, String attributeInstance, Double value) {
		Hashtable<String, Double> valCount = null;
		
		if(this.containsKey(attributeName)) {
			valCount = this.get(attributeName);
			if(valCount.containsKey(attributeInstance)) { // reduce count by value
				double currValue = valCount.get(attributeInstance);
				currValue -= value;
				// remove if value is 0
				if(currValue == 0) {
					valCount.remove(attributeInstance);
				} else {
					valCount.put(attributeInstance, currValue);
				}
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

//	@Override
//	public void setWeights(Map<String, Double> categoricalWeights) {
//		this.weights = categoricalWeights;
//	}

//	public void addWeight(String attributeName, Double weight) {
//		weights.put(attributeName, weight);
//	}
	
	@Override
	public Double getSimilarity(String attributeName, String attributeInstance) {
		return 0.0;
	}

//	@Override
//	public Double getSimilarity(List<String> attributeNames, List<String> attributeInstances) {
//		double similarity = 0.0;
//		// loop through all the categorical properties (each weight corresponds to one categorical property)
//		for(int i = 0; i < attributeNames.size(); i++) {
//			// sumProperties contains the total number of instances for the property
//			double sumProperties = 0;
//			Hashtable<String, Double> propertyHash = this.get(attributeNames.get(i));//categoryClusterInfo.get(i);
//			Collection<Double> valueCollection = propertyHash.values();
//			for(Double val : valueCollection) {
//				sumProperties += val;
//			}
//
//			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
//			double numOccuranceInCluster = 0;
//			if(propertyHash.containsKey(attributeInstances.get(i))) {
//				numOccuranceInCluster = propertyHash.get(attributeInstances.get(i));
//			}
//			
//			double weight = weights.get(attributeNames.get(i));
//			similarity += weight * numOccuranceInCluster / sumProperties;
//		}
//
//		return similarity;
//	}
	
	@Override
	public Double getSimilarity(List<String> attributeNames, List<String> attributeInstances, int indexToSkip) {
		double similarity = 0.0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < attributeNames.size(); i++) {
			if(i==indexToSkip) {
				continue;
			}
			// sumProperties contains the total number of instances for the property
			double sumProperties = 0;
			Hashtable<String, Double> propertyHash = this.get(attributeNames.get(i));//categoryClusterInfo.get(i);
			Collection<Double> valueCollection = propertyHash.values();
			for(Double val : valueCollection) {
				sumProperties += val;
			}

			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			double numOccuranceInCluster = 0;
			if(propertyHash.containsKey(attributeInstances.get(i))) {
				numOccuranceInCluster = propertyHash.get(attributeInstances.get(i));
			}
			
			double weight = weights.get(attributeNames.get(i));
			similarity += weight * numOccuranceInCluster / sumProperties;
		}

		return similarity;
	}
	
	@Override
	public void reset() {
		for(String key: this.keySet()) {
			Hashtable<String, Double> table = this.get(key);
			for(String key2: table.keySet()) {
				table.put(key2, 0.0);
			}
		}
	}

	@Override
	public double getClusterSimilarity(ICategoricalCluster c2, String instanceType) {
		double similarity = 0;
		for(String attributeType : this.keySet()) {
			if(attributeType.equals(instanceType)) {
				continue;
			}
			
			Hashtable<String, Double> thisTypeHash = this.get(attributeType);
			Hashtable<String, Double> typeHash = ((CategoricalCluster) c2).get(attributeType);
			
			if(thisTypeHash.isEmpty() || typeHash.isEmpty()) {
				continue;
			}
			
			double normalizationCount1 = 0;
			for(String propInstance : thisTypeHash.keySet()) {
				normalizationCount1 += thisTypeHash.get(propInstance);
			}
			double normalizationCount2 = 0;
			for(String propInstance : typeHash.keySet()) {
				normalizationCount2 += typeHash.get(propInstance);
			}

			int possibleValues = 0;
			double sumClusterDiff = 0;
			for(String propInstance : thisTypeHash.keySet()) {
				double count1 = thisTypeHash.get(propInstance);
				if(typeHash.containsKey(propInstance)) {
					possibleValues++;
					// calculate difference between counts
					double count2 = typeHash.get(propInstance);
					sumClusterDiff += Math.abs( count1/normalizationCount1 - count2/normalizationCount2);
				} else {
					possibleValues++;
					//include values that 1st cluster has and 2nd cluster doesn't have
					sumClusterDiff += count1/normalizationCount1;
				}
			}
			//now include values that 2nd cluster has that 1st cluster doesn't have
			for(String propInstance: typeHash.keySet()) {
				if(!thisTypeHash.containsKey(propInstance)) {
					possibleValues++;
					double count2 = typeHash.get(propInstance);
					sumClusterDiff += count2/normalizationCount2;
				}
			}
			
			similarity += weights.get(attributeType) * (1 - sumClusterDiff/possibleValues);
		}
		
		return similarity;
	}
	
	@Override
	public Map<String, Double> getWeights() {
		return this.weights;
	}
	
}
