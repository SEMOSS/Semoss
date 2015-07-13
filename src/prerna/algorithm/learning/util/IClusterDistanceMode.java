package prerna.algorithm.learning.util;

public interface IClusterDistanceMode {

	enum DistanceMeasure {MEAN, MODE, MEDIAN, MAX, MIN}
	
	double getCentroidValue();
	
	void addToCentroidValue(Double newValue);
	
	void removeFromCentroidValue(Double newValue);
	
	double getNullRatio();
	
	double getNumNull();
	
	double getNumInstances();
	
	double getPreviousCentroidValue();
	
	Double getChangeToCentroidValue();
	
	void reset();

	boolean isPreviousNull();

	void addPartialToCentroidValue(Double newValue, double factor);

	void removePartialFromCentroidValue(Double value, double factor);
}
