package prerna.algorithm.learning.util;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class NumericalCluster implements INumericalCluster {

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
	
	@Override
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
	
	@Override
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
	
	@Override
	public void addToCluster(List<String> attributeName, List<Double> value) {
		for(int i = 0; i < attributeName.size(); i++) {
			this.addToCluster(attributeName.get(i), value.get(i));
		}
	}
	
	@Override
	public void addToCluster(String attributeName, Double value) {
		IClusterDistanceMode distanceMeasure = distanceMeasureForAttribute.get(attributeName);
		distanceMeasure.addToCentroidValue(value);
	}

	@Override
	public void removeFromCluster(List<String> attributeName, List<Double> value) {
		for(int i = 0; i < attributeName.size(); i++) {
			this.removeFromCluster(attributeName.get(i), value.get(i));
		}	
	}
	
	@Override
	public void removeFromCluster(String attributeName, Double value) {
		IClusterDistanceMode distanceMeasure = distanceMeasureForAttribute.get(attributeName);
		distanceMeasure.removeFromCentroidValue(value);
	}

	@Override
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		distanceMeasureForAttribute.put(attributeName, distanceMeasure);
	}
	
	@Override
	public void reset() {
		for(String key: distanceMeasureForAttribute.keySet()) {
			distanceMeasureForAttribute.get(key).reset();
		}
	}

	@Override
	public boolean isEmpty() {
		return distanceMeasureForAttribute.isEmpty();
	}

	@Override
	public double getClusterSimilarity(INumericalCluster c2, String instanceType) {
		double similarity = 0;
		Map<String, IClusterDistanceMode> otherDistanceMeasureForAttribute = c2.getDistanceMeasureForAttribute();
		if(otherDistanceMeasureForAttribute.isEmpty() || this.distanceMeasureForAttribute.isEmpty()) {
			return similarity;
		}
		
		for(String attributeName : this.distanceMeasureForAttribute.keySet()) {
			if(attributeName.equals(instanceType)) {
				continue;
			}
			Double weight = weights.get(attributeName);

			IClusterDistanceMode thisDMeasure = this.distanceMeasureForAttribute.get(attributeName);
			Double thisCenterValue = thisDMeasure.getCentroidValue();
			IClusterDistanceMode dMeasure = otherDistanceMeasureForAttribute.get(attributeName);
			Double centerValue = dMeasure.getCentroidValue();
			if(thisCenterValue == null || centerValue == null) {
				int thisNumNull = thisDMeasure.getNumNull();
				int thisNumVals = thisDMeasure.getNumInstances();
				int numNull = dMeasure.getNumNull();
				int numVals = dMeasure.getNumInstances();
				similarity = similarity + (double) (thisNumNull + numNull) / (thisNumVals + numVals) * weight;
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

	@Override
	public Map<String, IClusterDistanceMode> getDistanceMeasureForAttribute() {
		return this.distanceMeasureForAttribute;
	}

	@Override
	public Map<String, Double> getWeights() {
		return this.weights;
	}
}
