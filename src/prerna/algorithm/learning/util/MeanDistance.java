package prerna.algorithm.learning.util;

public class MeanDistance implements IClusterDistanceMode {

	private double centroidValue;
	private int numInstances;
	private int emptyInstances;
	
	private double previousCentroidValue;
	private double changeToCentroidValue;
	private boolean previousNull;
	
	public MeanDistance() {

	}
	
	@Override
	public double getCentroidValue() {
		return this.centroidValue;
	}

	@Override
	public void addToCentroidValue(Double newValue) {
		if(newValue == null) {
			previousNull = true;
			emptyInstances++;
			return;
		}
		
		previousNull = false;
		previousCentroidValue = centroidValue;
		changeToCentroidValue = (newValue - previousCentroidValue) / (numInstances + 1);
		centroidValue += changeToCentroidValue;
		numInstances++;
	}
	
	@Override
	public void removeFromCentroidValue(Double newValue) {
		if(newValue == null) {
			previousNull = true;
			emptyInstances--;
			return;
		}
		
		previousNull = false;
		previousCentroidValue = centroidValue;
		changeToCentroidValue = (-1*newValue - previousCentroidValue) / (numInstances - 1);
		centroidValue += changeToCentroidValue;
		numInstances--;
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
		previousCentroidValue = 0;
		changeToCentroidValue = 0;
	}

	@Override
	public double getPreviousCentroidValue() {
		return this.previousCentroidValue;
	}

	@Override
	public double getChangeToCentroidValue() {
		return this.changeToCentroidValue;
	}
	
	@Override
	public int getNumNull() {
		return this.emptyInstances;
	}

	@Override
	public int getNumInstances() {
		return this.numInstances;
	}
	
	@Override
	public boolean isPreviousNull() {
		return this.previousNull;
	}
}
