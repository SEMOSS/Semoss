package prerna.algorithm.learning.util;

public class MeanDistance implements IClusterDistanceMode {

	private double centroidValue = 0;
	private int numInstances = 0;
	
	@Override
	public double getCentroidValue() {
		return this.centroidValue;
	}

	@Override
	public void addToCentroidValue(double newValue) {
		double currValue = centroidValue * numInstances;
		currValue += newValue;
		numInstances++;
		centroidValue = currValue / numInstances;
	}

	@Override
	public void removeFromCentroidValue(double newValue) {
		double currValue = centroidValue * numInstances;
		currValue -= newValue;
		numInstances--;
		centroidValue = currValue / numInstances;
	}

}
