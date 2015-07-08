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
//	public NumericalCluster(Map<String, Double> w, Map<String, Double> r) {
//		distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
//		weights = w;
//		ranges = r;
//	}
	
	/**
	 * Default constructor
	 */
	public NumericalCluster(Map<String, Double> w, Map<String, Double> r, Map<String, Double> m) {
		this.distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
		this.weights = w;
		this.ranges = r;
		this.mins = m;
	}
	
//	//TODO: reduce code redundancy in getSimilarity  Methods
//	@Override
//	public Double getSimilarity(List<String> attributeName, List<Double> value) {
//		double similarity = 0.0;
//		for(int i = 0; i < attributeName.size(); i++) {
//			String attribute = attributeName.get(i);
//			Double v = value.get(i);
//			if(v==null) {
//				v = distanceMeasureForAttribute.get(attribute).getNullRatio();
//			}
//			Double center = distanceMeasureForAttribute.get(attribute).getCentroidValue();
//			Double weight = weights.get(attribute);
//			Double range = ranges.get(attribute);
//			center = center/range;
//			
//			//using euclidean distance
//			similarity = similarity + (Math.pow(Math.abs(v-center), 2))*weight;
//		}
//		return Math.sqrt(similarity);
//	}
	
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
	public Double getSimilarity(String attributeName, Double value) {
		//TODO: return similarity score for one dimension
		return 0.0;
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

		
//		if(this.containsKey(attributeName)) { // old instance value for property
//			this.put(attributeName, distanceMeasure.getCentroidValue());
//		} else { // new instance value for property
//			this.put(attributeName, value);
//		}
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

//		if(this.containsKey(attributeName)) { // old instance value for property
//			this.put(attributeName, distanceMeasure.getCentroidValue());
//		} else { // new instance value for property
//			throw new NullPointerException("Attribute " + attributeName + " cannot be found in cluster to remove...");
//		}
	}

	@Override
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		distanceMeasureForAttribute.put(attributeName, distanceMeasure);
	}

//	@Override
//	public void setWeights(Map<String, Double> numericalWeights) {
//		weights = numericalWeights;
//	}
	
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
