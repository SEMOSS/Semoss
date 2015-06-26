package prerna.algorithm.learning.util;

import java.util.Hashtable;
import java.util.Map;

public class NumericalCluster extends Hashtable<String, Double> implements INumericalCluster {

	private Map<String, IClusterDistanceMode> distanceMeasureForAttribute;
	
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

}
