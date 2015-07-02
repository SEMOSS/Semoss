package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class NumericalCluster implements INumericalCluster {

	//TODO: consolidate these maps to save memory since Keys for all maps are identical
	private Map<String, IClusterDistanceMode> distanceMeasureForAttribute;
	private Map<String, Double> weights;
	private Map<String, Double> ranges;
	
	/**
	 * Default constructor
	 */
	public NumericalCluster(Map<String, Double> w, Map<String, Double> r) {
		distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
		weights = w;
		ranges = r;
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
				return weight*v;
			}
			Double center = distanceMeasureForAttribute.get(attribute).getCentroidValue();
			Double range = ranges.get(attribute);
			center = center/range;
			v = v/range;
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
}
