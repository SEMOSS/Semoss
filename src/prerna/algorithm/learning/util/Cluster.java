package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Cluster {

	private ICategoricalCluster categoricalCluster;
	private INumericalCluster numericalCluster;
	
	private final Map<String, Double> categoricalWeights;
	private final Map<String, Double> numericalWeights;
	
	public Cluster(Map<String, Double> categoricalWeights, Map<String, Double> numericalWeights) {
		this.categoricalCluster = new CategoricalCluster();
		this.numericalCluster = new NumericalCluster();
		categoricalCluster.setWeights(categoricalWeights);
		numericalCluster.setWeights(numericalWeights);
		this.categoricalWeights = categoricalWeights;
		this.numericalWeights = numericalWeights;
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
	
	/**
	 * Returns the similarity score for an instance with the this cluster
	 * @param instanceValues			The values for the instance for each column
	 * @param attributeNames			The names of associated for each value the instanceValues array, essentially the 'column name'
	 * @param isNumeric					The boolean representing if value of i in the array is numeric
	 * @return
	 */
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
		
		if(!numericalValues.isEmpty()) {
			similarityValue += getSimilarityFromNumericalValues(numericalValues, numericalValueNames);
		}
		
		return similarityValue;
	}

	private double getSimilarityFromNumericalValues(List<Double> numericalValues, List<String> numericalValueNames) {
		// TODO Auto-generated method stub
		return numericalCluster.getSimilarity(numericalValueNames, numericalValues);
	}

	private double getSimilarityFromCategoricalValues(List<String> categoricalValues, List<String> categoricalValueNames) {
		// TODO Auto-generated method stub
		return categoricalCluster.getSimilarity(categoricalValues, categoricalValueNames);
	}
}
