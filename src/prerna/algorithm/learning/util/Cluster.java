package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Cluster {

	private ICategoricalCluster categoricalCluster;
	private INumericalCluster numericalCluster;
	
	private Map<String, Double> categoricalWeights;
	private Map<String, Double> numericalWeights;
	
	public Cluster() {
		this.categoricalCluster = new CategoricalCluster();
		this.numericalCluster = new NumericalCluster();
	}
	
	public void addToCategoricalCluster(String attributeName, String attributeInstance, Double value) {
		categoricalCluster.addToCluster(attributeName, attributeInstance, value);
	}
	
	public void removeFromCategoricalCluster(String attributeName, String attributeInstance, Double value) {
		categoricalCluster.removeFromCluster(attributeName, attributeInstance, value);
	}
	
	public void addToNumericalCluster(String attributeName, Double value) {
		numericalCluster.addToCluster(attributeName, value);
	}
	
	public void removeFromNumericalCluster(String attributeName, Double value) {
		numericalCluster.removeFromCluster(attributeName, value);
	}
	
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		numericalCluster.setDistanceMode(attributeName, distanceMeasure);
	}
	
	public double getSimilarityForInstance(Object[] instanceValues, String[] attributeNames, boolean[] isNumeric) {
		List<String> categoricalValues = new Vector<String>();
		List<String> categoricalValueNames = new Vector<String>();
		List<Double> numericalValues = new Vector<Double>();
		List<String> numericalValueNames = new Vector<String>();
		
		int i = 0;
		int size = isNumeric.length;
		for(; i < size; i++) {
			if(isNumeric[i]) {
				numericalValues.add((Double) instanceValues[i]);
				numericalValueNames.add((String) attributeNames[i]);	
			} else {
				categoricalValues.add((String) instanceValues[i]);
				categoricalValueNames.add((String) attributeNames[i]);
			}
		}
		
		double similarityValue = 0;
		
		if(!categoricalValues.isEmpty()) {
			similarityValue += getSimilarityFromCategoricalValues(categoricalValues, categoricalValueNames);
		}
		
		if(numericalValues.isEmpty()) {
			similarityValue += getSimilarityFromNumericalValues(categoricalValues, categoricalValueNames);
		}
		
		return similarityValue;
	}

	private double getSimilarityFromNumericalValues(List<String> categoricalValues, List<String> categoricalValueNames) {
		// TODO Auto-generated method stub
		return 0;
	}

	private double getSimilarityFromCategoricalValues(List<String> categoricalValues, List<String> categoricalValueNames) {
		// TODO Auto-generated method stub
		return 0;
	}
}
