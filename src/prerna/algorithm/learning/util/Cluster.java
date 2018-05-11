package prerna.algorithm.learning.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class Cluster {

	private int numInstances;
	private CategoricalCluster categoricalCluster;
	private NumericalCluster numericalCluster;
	
	private static final String EMPTY = "_____";
	
	/**
	 * Cluster that auto-generates the weighting to be equal among
	 */
	public Cluster(String[] attributeNames, boolean[] isNumeric) {
		Map<String, Double> numericalWeights = new HashMap<String, Double>();
		Map<String, Double> categoricalWeights = new HashMap<String, Double>();
		// making all columns equally weighted
		int numAttributes = isNumeric.length;
		for(int i = 0; i < numAttributes; i++) {
			if(isNumeric[i]) {
				numericalWeights.put(attributeNames[i], 1.0);
			} else {
				categoricalWeights.put(attributeNames[i], 1.0);
			}
		}
		this.categoricalCluster = new CategoricalCluster(categoricalWeights);
		this.numericalCluster = new NumericalCluster(numericalWeights);
	}
	
	/**
	 * Use custom weighting on the columns 
	 * @param categoricalWeights
	 * @param numericalWeights
	 */
	public Cluster(Map<String, Double> categoricalWeights, Map<String, Double> numericalWeights) {
		this.categoricalCluster = new CategoricalCluster(categoricalWeights);
		this.numericalCluster = new NumericalCluster(numericalWeights);
	}

	public void addToCluster(List<Object[]> valuesList, String[] names, boolean[] isNumeric, double factor) {
		//numInstances++; do not increase with incomplete additions
		
		// skip all the duplication logic and add directly
		if(valuesList.size() == 1) {
			addToCluster(valuesList.get(0), names, isNumeric);
		}
		
		Double[] numVals = new Double[valuesList.get(0).length];
		for(Object[] values : valuesList) {
			for(int i = 0; i < values.length; i++){
				if(isNumeric[i]) {
					if(values[i]==null || values[i].equals(EMPTY)) {
						// do nothing, keep null
					} else {
						//TODO: determine if should be using Mean/Max/Min/Sum/Median
						//implemented to always assume mean at the moment
						if(numVals[i] == null) {
							numVals[i] = 0.0;
						}
						numVals[i] += (double) values[i];
					}
				} else {
					if(values[i]==null || values[i].equals(EMPTY)){
						addToCategoricalCluster(names[i], EMPTY, factor);
					} else {
						addToCategoricalCluster(names[i], values[i] + "", factor);
					}
				}
			}
		}
		
		int size = valuesList.size();
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				if(numVals[i] == null) {
					addToNumericalCluster(names[i], null, factor);
				} else {
					addToNumericalCluster(names[i], numVals[i] / size, factor);
				}
			}
		}
		
	}
	
	public void addToCluster(List<Object[]> valuesList, String[] names, boolean[] isNumeric) {
		numInstances++;
		
		// skip all the duplication logic and add directly
		if(valuesList.size() == 1) {
			addToCluster(valuesList.get(0), names, isNumeric);
		}
		
		Double[] numVals = new Double[valuesList.get(0).length];
		for(Object[] values : valuesList) {
			for(int i = 0; i < values.length; i++){
				if(isNumeric[i]) {
					if(values[i]==null || values[i].equals(EMPTY)) {
						// do nothing, keep null
					} else {
						//TODO: determine if should be using Mean/Max/Min/Sum/Median
						//implemented to always assume mean at the moment
						if(numVals[i] == null) {
							numVals[i] = 0.0;
						}
						numVals[i] += ((Number) values[i]).doubleValue();
					}
				} else {
					if(values[i]==null || values[i].equals(EMPTY)){
						addToCategoricalCluster(names[i], EMPTY);
					} else {
						addToCategoricalCluster(names[i], values[i] + "");
					}
				}
			}
		}
		
		int size = valuesList.size();
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				if(numVals[i] == null) {
					addToNumericalCluster(names[i], null);
				} else {
					addToNumericalCluster(names[i], numVals[i] / size);
				}
			}
		}
	}
	
	private void addToCluster(Object[] values, String[] names, boolean[] isNumeric) {
		for(int i = 0; i < values.length; i++){
			if(isNumeric[i]) {
				if(values[i]==null || values[i].equals(EMPTY)) {
					addToNumericalCluster(names[i], null);
				} else {
					addToNumericalCluster(names[i], ((Number) values[i]).doubleValue());
				}
			} else {
				if(values[i]==null || values[i].equals(EMPTY)){
					addToCategoricalCluster(names[i], EMPTY);
				} else {
					addToCategoricalCluster(names[i], values[i] + "");
				}
			}
		}
	}
	
	public void removeFromCluster(List<Object[]> valuesList, String[] names, boolean[] isNumeric, double factor) {
		//numInstances--; do not decrease with incomplete additions

		// skip all the duplication logic and add directly
		if(valuesList.size() == 1) {
			removeFromCluster(valuesList.get(0), names, isNumeric);
		}
		
		Double[] numVals = new Double[valuesList.get(0).length];
		for(Object[] values : valuesList) {
			for(int i = 0; i < values.length; i++){
				if(isNumeric[i]) {
					if(values[i]==null || values[i].equals(EMPTY)) {
						// do nothing, keep null
					} else {
						//TODO: determine if should be using Mean/Max/Min/Sum/Median
						//implemented to always assume mean at the moment
						if(numVals[i] == null) {
							numVals[i] = 0.0;
						}
						numVals[i] += ((Number) values[i]).doubleValue();
					}
				} else {
					removeFromCategoricalCluster(names[i], values[i] + "", factor);
				}
			}
		}
		
		int size = valuesList.size();
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				if(numVals[i] == null) {
					removeFromNumericalCluster(names[i], null, factor);
				} else {
					removeFromNumericalCluster(names[i], numVals[i] / size, factor);
				}
			}
		}
	}
	
	public void removeFromCluster(List<Object[]> valuesList, String[] names, boolean[] isNumeric) {
		numInstances--;

		// skip all the duplication logic and add directly
		if(valuesList.size() == 1) {
			removeFromCluster(valuesList.get(0), names, isNumeric);
		}
		
		Double[] numVals = new Double[valuesList.get(0).length];
		for(Object[] values : valuesList) {
			for(int i = 0; i < values.length; i++){
				if(isNumeric[i]) {
					if(values[i]==null || values[i].equals(EMPTY)) {
						// do nothing, keep null
					} else {
						//TODO: determine if should be using Mean/Max/Min/Sum/Median
						//implemented to always assume mean at the moment
						if(numVals[i] == null) {
							numVals[i] = 0.0;
						}
						numVals[i] += ((Number) values[i]).doubleValue();
					}
				} else {
					removeFromCategoricalCluster(names[i], values[i] + "");
				}
			}
		}
		
		int size = valuesList.size();
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				if(numVals[i] == null) {
					removeFromNumericalCluster(names[i], null);
				} else {
					removeFromNumericalCluster(names[i], numVals[i] / size);
				}
			}
		}
	}
	
	private void removeFromCluster(Object[] values, String[] names, boolean[] isNumeric) {
		for(int i = 0; i < values.length; i++){
			if(isNumeric[i]) {
				if(values[i]==null || values[i].equals(EMPTY)) {
					removeFromNumericalCluster(names[i], null);
				} else {
					removeFromNumericalCluster(names[i], (Double)values[i]);
				}
			} else {
				removeFromCategoricalCluster(names[i], values[i] + "");
			}
		}
	}
	
	private void addToCategoricalCluster(String attributeName, String attributeInstance, double factor) {
		categoricalCluster.addToCluster(attributeName, attributeInstance, factor);
	}
	
	private void removeFromCategoricalCluster(String attributeName, String attributeInstance, double factor) {
		categoricalCluster.removeFromCluster(attributeName, attributeInstance, factor);
	}
	
	private void addToCategoricalCluster(String attributeName, String attributeInstance) {
		categoricalCluster.addToCluster(attributeName, attributeInstance, 1.0);
	}
	
	private void removeFromCategoricalCluster(String attributeName, String attributeInstance) {
		categoricalCluster.removeFromCluster(attributeName, attributeInstance, 1.0);
	}
	
	private void addToNumericalCluster(String attributeName, Double value, double factor) {
		numericalCluster.addToCluster(attributeName, value, factor);
	}
	
	private void removeFromNumericalCluster(String attributeName, Double value, double factor) {
		numericalCluster.removeFromCluster(attributeName, value, factor);
	}
	
	private void addToNumericalCluster(String attributeName, Double value) {
		numericalCluster.addToCluster(attributeName, value);
	}
	
	private void removeFromNumericalCluster(String attributeName, Double value) {
		numericalCluster.removeFromCluster(attributeName, value);
	}
	
	public void setDistanceMode(Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasures) {
		if(distanceMeasures != null) {
			for(String attribute : distanceMeasures.keySet()) {
				IClusterDistanceMode distance = null;
				IClusterDistanceMode.DistanceMeasure measure = distanceMeasures.get(attribute);
				if(measure == IClusterDistanceMode.DistanceMeasure.MEAN) {
					distance = new MeanDistance();
				} 
				//TODO: add other classes once they are done being implemented
	//			else if(measure == IClusterDistanceMode.DistanceMeasure.MAX) {
	//				
	//			}
	//			else if(measure == IClusterDistanceMode.DistanceMeasure.MEDIAN) {
	//				
	//			}
				setDistanceMode(attribute, distance);
			}
		}
	}
	
	public void setDistanceMode(String attributeName, IClusterDistanceMode distanceMeasure) {
		numericalCluster.setDistanceMode(attributeName, distanceMeasure);
	}
	
	/**
	 * Returns the similarity score for an instance with this cluster
	 * @param instanceValues			The values for the instance for each column
	 * @param attributeNames			The names of associated for each value the instanceValues array, essentially the 'column name'
	 * @param isNumeric					The boolean representing if value of i in the array is numeric
	 * @return
	 */
	public double getSimilarityForInstance(List<Object[]> instanceValues, String[] attributeNames, boolean[] isNumeric, int indexToSkip) {
		double similarityValue = 0;
		for(Object[] values : instanceValues) {
			similarityValue += getSimilarityForInstance(values, attributeNames, isNumeric, indexToSkip);
		}
		
		return similarityValue / instanceValues.size();
	}

	/**
	 * Returns the similarity score for an instance with the this cluster
	 * @param instanceValues			The values for the instance for each column
	 * @param attributeNames			The names of associated for each value the instanceValues array, essentially the 'column name'
	 * @param isNumeric					The boolean representing if value of i in the array is numeric
	 * @return
	 */
	public double getSimilarityForInstance(Object[] instanceValues, String[] attributeNames, boolean[] isNumeric, int instanceIndex) {
		List<String> categoricalValues = new Vector<String>();
		List<String> categoricalValueNames = new Vector<String>();
		List<Double> numericalValues = new Vector<Double>();
		List<String> numericalValueNames = new Vector<String>();
		
		int i = 0;
		int size = isNumeric.length;
		int indexToSkip = -1;
		for(; i < size; i++) {
			if(isNumeric[i]) {
				numericalValues.add( ((Number) instanceValues[i]).doubleValue());
				numericalValueNames.add(attributeNames[i] + "");
				if(i==instanceIndex) indexToSkip = numericalValueNames.size()-1;
			} else {
				categoricalValues.add(instanceValues[i] + "");
				categoricalValueNames.add(attributeNames[i] + "");
				if(i==instanceIndex) indexToSkip = categoricalValueNames.size()-1;
			}
		}
		
		// since the attribute names includes the instance
		// we need to appropriately calculate the similarity
		// based on the number of categorical vs. numerical values
		int totalAttributes = attributeNames.length-1;
		
		double categoricalSimVal = 0;
		if(!categoricalValues.isEmpty()) {
			if(isNumeric[instanceIndex]) {
				categoricalSimVal = ((double) categoricalValues.size() / totalAttributes) * getSimilarityFromCategoricalValues(categoricalValues, categoricalValueNames, -1);
			} else {
				categoricalSimVal = ((double) (categoricalValues.size()-1) / totalAttributes) * getSimilarityFromCategoricalValues(categoricalValues, categoricalValueNames, indexToSkip);
			}
		}
		
		double numericalSimVal = 0;
		if(!numericalValues.isEmpty()) {
			if(isNumeric[instanceIndex]) {
				numericalSimVal = ((double) (numericalValues.size()-1) / totalAttributes) * getSimilarityFromNumericalValues(numericalValues, numericalValueNames, indexToSkip);
			} else {
				numericalSimVal = ((double) numericalValues.size() / totalAttributes) * getSimilarityFromNumericalValues(numericalValues, numericalValueNames, -1);
			}
		}
		
		return categoricalSimVal + numericalSimVal;
	}

	private double getSimilarityFromNumericalValues(List<Double> numericalValues, List<String> numericalValueNames, int index) {
		return numericalCluster.getSimilarity(numericalValueNames, numericalValues, index);
	}
	
	private double getSimilarityFromCategoricalValues(List<String> categoricalValues, List<String> categoricalValueNames, int index) {
		return categoricalCluster.getSimilarity(categoricalValueNames, categoricalValues, index);
	}
	
	public double getClusterSimilarity(Cluster c2, String instanceType) {
		int numCategorical = 0;
		int numNumeric = 0;
		if(this.categoricalCluster.getWeights() != null) {
			Set<String> catKeys = this.categoricalCluster.getWeights().keySet();
			numCategorical = catKeys.size();
			if(catKeys.contains(instanceType)) {
				numCategorical--;
			}
		}
		if(this.numericalCluster.getWeights() != null) {
			Set<String> numKeys = this.numericalCluster.getWeights().keySet();
			numNumeric = numKeys.size();
			if(numKeys.contains(instanceType)) {
				numNumeric--;
			}
		}
		int totalAttributes = numCategorical + numNumeric;
		
		double numericalClusterSim = ((double) numNumeric / totalAttributes) * this.numericalCluster.getClusterSimilarity(c2.numericalCluster, instanceType);
		double categoricalClusterSim = ((double) numCategorical / totalAttributes) * this.categoricalCluster.getClusterSimilarity(c2.categoricalCluster, instanceType);
		return numericalClusterSim + categoricalClusterSim;
	}
	
	public int getNumInstances() {
		return this.numInstances;
	}
	
	public void setDistanceMode(String[] levelNames, IClusterDistanceMode[] distanceMeasures, boolean[] isNumeric) {
		for(int i = 0; i < levelNames.length; i++) {
			if(isNumeric[i]) {
				setDistanceMode(levelNames[i], distanceMeasures[i]);
			}
		}
	}
	
	public void setDistanceMode(String[] levelNames, IClusterDistanceMode[] distanceMeasures) {
		for(int i = 0; i < levelNames.length; i++) {
			setDistanceMode(levelNames[i], distanceMeasures[i]);
		}
	}
	
}
