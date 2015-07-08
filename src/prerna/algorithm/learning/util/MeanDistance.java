package prerna.algorithm.learning.util;

public class MeanDistance implements IClusterDistanceMode {

	private double centroidValue;
	private int numInstances;
	private int emptyInstances;
	
	public MeanDistance() {
		centroidValue = 0;
		numInstances = 0;
		emptyInstances = 0;
	}
	
	@Override
	public double getCentroidValue() {
		return this.centroidValue;
	}

	@Override
	public void addToCentroidValue(Double newValue) {
		double currValue = centroidValue * numInstances;
		
		if(newValue != null) {
			currValue += newValue;
			numInstances++;
		} else {
			emptyInstances++;
		}
		
		centroidValue = currValue / (numInstances+emptyInstances);
	}

	@Override
	public void removeFromCentroidValue(Double newValue) {
		double currValue = centroidValue * numInstances;
		currValue -= newValue;
		numInstances--;
		centroidValue = currValue / numInstances;
	}
	
	@Override
	public double getNullRatio() {
		double e = (double)emptyInstances;
		double i = (double)numInstances;
		double total = e+i;
		if(total == 0) {
			return 0;
		} else {
			return e/(e+i);
		}
	}

	@Override
	public void reset() {
		centroidValue = 0;
		numInstances = 0;
		emptyInstances = 0;
	}

	@Override
	public int getNumNull() {
		return emptyInstances;
	}

	@Override
	public int getNumInstances() {
		return numInstances;
	}
}
