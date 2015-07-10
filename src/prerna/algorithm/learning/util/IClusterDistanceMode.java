package prerna.algorithm.learning.util;

public interface IClusterDistanceMode {

	enum DistanceMeasure {MEAN, MODE, MEDIAN, MAX, MIN}
	
	double getCentroidValue();
	
	void addToCentroidValue(Double newValue);
	
	void removeFromCentroidValue(Double newValue);
	
	double getNullRatio();
	
	int getNumNull();
	
	int getNumInstances();
	
	double getPreviousCentroidValue();
	
	double getChangeToCentroidValue();
	
	void reset();

	boolean isPreviousNull();
}
