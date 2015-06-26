package prerna.algorithm.learning.util;

public interface IClusterDistanceMode {

	enum DistanceMeasure {MEAN, MODE, MEDIAN, MAX, MIN}
	
	double getCentroidValue();
	
	void addToCentroidValue(double newValue);
	
	void removeFromCentroidValue(double newValue);
}
