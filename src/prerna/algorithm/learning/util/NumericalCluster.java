package prerna.algorithm.learning.util;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class NumericalCluster {

	//TODO: consolidate these maps to save memory since Keys for all maps are identical
	private Map<String, IClusterDistanceMode> distanceMeasureForAttribute;
	private Map<String, Double> weights = new HashMap<String, Double>();
	private Map<String, Double> ranges = new HashMap<String, Double>();
	private Map<String, Double> mins = new HashMap<String, Double>();

	/**
	 * Default constructor
	 */
	public NumericalCluster(Map<String, Double> w, Map<String, Double> r, Map<String, Double> m) {
		this.distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
		this.weights = w;
		this.ranges = r;
		this.mins = m;
	}
	
	public Double getSimilarity(List<String> attributeName, List<Double> value, int indexToSkip) {
		double similarity = 0.0;
		int numAttr = attributeName.size();
		for(int i = 0; i < numAttr; i++) {
			if(i==indexToSkip) {
				continue;
			}
			String attribute = attributeName.get(i);
			Double v = value.get(i);
			Double weight = weights.get(attribute);
			if(v==null) {
				v = distanceMeasureForAttribute.get(attribute).getNullRatio();
				similarity = similarity + v*weight;
			}
			Double center = distanceMeasureForAttribute.get(attribute).getCentroidValue();
			Double range = ranges.get(attribute);
			Double min = mins.get(attribute);

			center = (center - min)/range;
			v = (v - min)/range;
			//using euclidean distance
			similarity = similarity + (Math.pow(v-center, 2))*weight;
		}
		return (1 - Math.sqrt(similarity));
	}
	
	public Double getSimilarity(String attribute, Double value) {
		Double weight = weights.get(attribute);
		if(value==null) {
			value = distanceMeasureForAttribute.get(attribute).getNullRatio();
			return value*weight;
		}
		Double center = distanceMeasureForAttribute.get(attribute).getCentroidValue();
		Double range = ranges.get(attribute);
		Double min = mins.get(attribute);

		center = (center - min)/range;
		value = (value - min)/range;
		//using euclidean distance
		return 1 - Math.sqrt((Math.pow(value-center, 2))*weight);
	}
	
	public void addToCluster(List<String> attributeName, List<Double> value) {
		for(int i = 0; i < attributeName.size(); i++) {
			this.addToCluster(attributeName.get(i), value.get(i));
		}
	}
	
	public void addToCluster(String attributeName, Double value) {
		IClusterDistanceMode distanceMeasure = distanceMeasureForAttribute.get(attributeName);
		distanceMeasure.addToCentroidValue(value);
	}

	public void removeFromCluster(List<String> attributeName, List<Double> value) {
		for(int i = 0; i < attributeName.size(); i++) {
			this.removeFromCluster(attributeName.get(i), value.get(i));
		}	
	}
	
	public void removeFromCluster(String attributeName, Double value) {
		IClusterDistanceMode distanceMeasure = distanceMeasureForAttribute.get(attributeName);
		distanceMeasure.removeFromCentroidValue(value);
	}

	public double getClusterSimilarity(NumericalCluster c2, String instanceType) {
		double similarity = 0;
		if(c2.isEmpty() || this.isEmpty()) {
			return similarity;
		}
		
		for(String attributeName : this.distanceMeasureForAttribute.keySet()) {
			if(attributeName.equals(instanceType)) {
				continue;
			}
			Double weight = weights.get(attributeName);

			Double thisCenterValue = this.getCenterValueForAttribute(attributeName);
			Double centerValue = c2.getCenterValueForAttribute(attributeName);
			if(thisCenterValue == null || centerValue == null) {
				int thisNumNull = this.getCenterNumNullsForAttribute(attributeName);
				int thisNumVals = this.getCenterNumInstancesForAttribute(attributeName);
				int numNull = c2.getCenterNumNullsForAttribute(attributeName);
				int numVals = c2.getCenterNumInstancesForAttribute(attributeName);
				similarity = similarity + (double) (thisNumNull + numNull) / (thisNumVals + numVals + thisNumNull + numNull) * weight;
			}
			
			Double range = ranges.get(attributeName);
			Double min = mins.get(attributeName);
			
			thisCenterValue = (thisCenterValue - min)/range;
			centerValue = (centerValue - min)/range;
			//using euclidean distance
			similarity = similarity + (Math.pow(thisCenterValue - centerValue, 2))*weight;
		}
		
		similarity = (1 - Math.sqrt(similarity));
		return similarity;
	}

	public Map<String, Double> getWeights() {
		return this.weights;
	}
	
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		distanceMeasureForAttribute.put(attributeName, distanceMeasure);
	}
	
	public void reset() {
		for(String key: distanceMeasureForAttribute.keySet()) {
			distanceMeasureForAttribute.get(key).reset();
		}
	}

	public boolean isEmpty() {
		return distanceMeasureForAttribute.isEmpty();
	}
	
	//////////////START METHODS TO EXTRACT INFORMATION FROM DISTANCE MEASURE//////////////
	public Double getCenterValueForAttribute(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).getCentroidValue();
	}
	
	public int getCenterNumInstancesForAttribute(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).getNumInstances();
	}
	
	public int getCenterNumNullsForAttribute(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).getNumNull();
	}
	
	public double getPreviousCenterValueForAttribute(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).getPreviousCentroidValue();
	}
	
	public double getPreviousChangeToCenterForAttribute(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).getChangeToCentroidValue();
	}
	
	public boolean getIsPreviousNull(String attributeName) {
		return this.distanceMeasureForAttribute.get(attributeName).isPreviousNull();
	}
	//////////////END METHODS TO EXTRACT INFORMATION FROM DISTANCE MEASURE//////////////
	
}
