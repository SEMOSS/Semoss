package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class NumericalCluster extends Hashtable<String, Double> implements INumericalCluster {

	private Map<String, IClusterDistanceMode> distanceMeasureForAttribute;
	private Map<String, Double> weights;

	/**
	 * serialization id
	 */
	private static final long serialVersionUID = -4280174932550461526L;
	
	/**
	 * Default constructor
	 */
	public NumericalCluster(Map<String, Double> w) {
		distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
		weights = w;
	}
	
	@Override
	public Double getSimilarity(List<String> attributeName, List<Double> value) {
		double similarity = 0.0;
		for(int i = 0; i < attributeName.size(); i++) {
			String attribute = attributeName.get(i);
			double v = value.get(i);
			double center = distanceMeasureForAttribute.get(attribute).getCentroidValue();
			Double weight = weights.get(attribute);
			
			//using euclidean distance
			similarity = similarity + (Math.pow(Math.abs(v-center), 2))*weight;
		}
		return Math.sqrt(similarity);
	}
	
	@Override
	public Double getSimilarity(String attributeName, Double value) {
		/*
		List<String> a = new ArrayList<>(1);
		List<Double> v = new ArrayList<>(1);
		a.add(attributeName);
		v.add(value);
		return getSimilarity(a, v);
		*/
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

		if(this.containsKey(attributeName)) { // old instance value for property
			this.put(attributeName, distanceMeasure.getCentroidValue());
		} else { // new instance value for property
			this.put(attributeName, value);
		}
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

		if(this.containsKey(attributeName)) { // old instance value for property
			this.put(attributeName, distanceMeasure.getCentroidValue());
		} else { // new instance value for property
			throw new NullPointerException("Attribute " + attributeName + " cannot be found in cluster to remove...");
		}
	}

	@Override
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		distanceMeasureForAttribute.put(attributeName, distanceMeasure);
	}

	@Override
	public void setWeights(Map<String, Double> numericalWeights) {
		weights = numericalWeights;
	}
	
	public void setWeights(List<String> names, List<Double> weights) {
		
	}
}
