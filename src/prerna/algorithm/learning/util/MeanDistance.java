package prerna.algorithm.learning.util;

/**
 * Implementation of {@link IClusterDistanceMode} that calculates cluster centroids using the mean (average) value.
 * This class provides incremental mean calculation, allowing for efficient addition and removal of values
 * while maintaining the running average and tracking null values separately.
 * 
 * <p>
 * The MeanDistance class supports both full and partial updates, making it suitable for
 * dynamic clustering scenarios where data points may be added or removed with fractional weights.
 * It maintains state information about previous operations to enable undo functionality.
 * </p>
 * 
 * @see {@link IClusterDistanceMode} for the interface contract
 * @see {@link NumericalCluster} for usage in numerical clustering
 */
public class MeanDistance implements IClusterDistanceMode {

	/** The current mean value of the cluster centroid. */
	private double centroidValue;
	
	/** The number of non-null instances contributing to the mean calculation. */
	private double numInstances;
	
	/** The number of null/empty instances encountered. */
	private double emptyInstances;
	
	/** The centroid value before the most recent update operation. */
	private double previousCentroidValue;
	
	/** The amount of change applied in the most recent update operation. */
	private double changeToCentroidValue;
	
	/** Flag indicating whether the most recent operation involved a null value. */
	private boolean previousNull;
	
	/**
	 * Constructs a new MeanDistance instance with all values initialized to zero.
	 */
	public MeanDistance() {

	}
	
	@Override
	public double getCentroidValue() {
		return this.centroidValue;
	}

	@Override
	public void addPartialToCentroidValue(Double newValue, double factor) {
		if(newValue == null) {
			previousNull = true;
			emptyInstances += factor;
			return;
		}
		
		previousNull = false;
		previousCentroidValue = centroidValue;
		changeToCentroidValue = (newValue*factor - previousCentroidValue) / (numInstances + 1);
		centroidValue += changeToCentroidValue;
		//numInstances++; do not increase numInstances for partial additions
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
	public void removePartialFromCentroidValue(Double newValue, double factor) {
		if(newValue == null) {
			previousNull = true;
			emptyInstances -= factor;
			return;
		}
		
		previousNull = false;
		previousCentroidValue = centroidValue;
		if(numInstances == 1) {
			changeToCentroidValue = -1*centroidValue;
			centroidValue = 0;
		} else {
			changeToCentroidValue = (-1*newValue*factor + previousCentroidValue) / (numInstances - 1);
			centroidValue += changeToCentroidValue;
		}
		//numInstances--; do not decrease numInstances for partial additions
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
		if(numInstances == 1) {
			changeToCentroidValue = -1*centroidValue;
			centroidValue = 0;
		} else {
			changeToCentroidValue = (-1*newValue + previousCentroidValue) / (numInstances - 1);
			centroidValue += changeToCentroidValue;
		}
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
	public Double getChangeToCentroidValue() {
		if(previousNull) {
			return null;
		}
		return this.changeToCentroidValue;
	}
	
	@Override
	public double getNumNull() {
		return this.emptyInstances;
	}

	@Override
	public double getNumInstances() {
		return this.numInstances;
	}
	
	@Override
	public boolean isPreviousNull() {
		return this.previousNull;
	}
}
