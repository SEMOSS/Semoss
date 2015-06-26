package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class NumericalCluster extends Hashtable<String, Double> implements INumericalCluster {

	private Map<String, IClusterDistanceMode> distanceMeasureForAttribute;
	private Map<String, Double> entropyValues;
	private Map<String, Double> weights;

	
	/**
	 * serialization id
	 */
	private static final long serialVersionUID = -4280174932550461526L;
	
	/**
	 * Default constructor
	 */
	public NumericalCluster() {
		distanceMeasureForAttribute = new Hashtable<String, IClusterDistanceMode>();
	}
	
	@Override
	public Double getSimilarity(List<String> attributeName, List<Double> value) {
		
		for(int i = 0; i < attributeName.size(); i++) {
			
			String attribute = attributeName.get(i);
			IClusterDistanceMode distanceMeasure = distanceMeasureForAttribute.get(attribute);
			Double weight = weights.get(attribute);
			
		}
		
		return 0.0;
	}
	
	@Override
	public Double getSimilarity(String attributeName, Double value) {
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
}
