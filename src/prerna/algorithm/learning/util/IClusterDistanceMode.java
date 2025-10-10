package prerna.algorithm.learning.util;

/**
 * Interface that defines the contract for cluster distance calculation modes.
 * This interface provides methods for managing centroid values and tracking
 * changes in cluster characteristics for numerical attributes.
 * 
 * <p>
 * Implementations of this interface handle different statistical measures
 * for calculating cluster centroids, such as mean, median, mode, maximum,
 * or minimum values. The interface supports both full and partial updates
 * to handle incremental clustering operations.
 * </p>
 * 
 * @see {@link MeanDistance} for mean-based distance calculation
 * @see {@link NumericalCluster} for usage in numerical clustering
 */
public interface IClusterDistanceMode {

	/** Enumeration of available distance calculation methods for cluster centroids. */
	enum DistanceMeasure {
		/** Mean-based distance calculation. */
		MEAN, 
		/** Mode-based distance calculation. */
		MODE, 
		/** Median-based distance calculation. */
		MEDIAN, 
		/** Maximum value-based distance calculation. */
		MAX, 
		/** Minimum value-based distance calculation. */
		MIN
	}
	
	/**
	 * Gets the current centroid value for this cluster attribute.
	 *
	 * @return The current centroid value.
	 */
	double getCentroidValue();
	
	/**
	 * Adds a new value to the centroid calculation.
	 *
	 * @param newValue The value to add to the centroid calculation.
	 */
	void addToCentroidValue(Double newValue);
	
	/**
	 * Removes a value from the centroid calculation.
	 *
	 * @param newValue The value to remove from the centroid calculation.
	 */
	void removeFromCentroidValue(Double newValue);
	
	/**
	 * Gets the ratio of null values to total instances.
	 *
	 * @return The ratio of null values (between 0.0 and 1.0).
	 */
	double getNullRatio();
	
	/**
	 * Gets the number of null values encountered.
	 *
	 * @return The count of null values.
	 */
	double getNumNull();
	
	/**
	 * Gets the total number of instances processed.
	 *
	 * @return The total instance count.
	 */
	double getNumInstances();
	
	/**
	 * Gets the previous centroid value before the last update.
	 *
	 * @return The previous centroid value.
	 */
	double getPreviousCentroidValue();
	
	/**
	 * Gets the change in centroid value from the last update.
	 *
	 * @return The change in centroid value, or null if the previous value was null.
	 */
	Double getChangeToCentroidValue();
	
	/**
	 * Resets all internal state to initial values.
	 */
	void reset();

	/**
	 * Determines if the previous update involved a null value.
	 *
	 * @return True if the previous value was null, false otherwise.
	 */
	boolean isPreviousNull();

	/**
	 * Adds a partial value to the centroid calculation with a specified weight factor.
	 *
	 * @param newValue The value to add to the centroid calculation.
	 * @param factor The weight factor for this partial addition.
	 */
	void addPartialToCentroidValue(Double newValue, double factor);

	/**
	 * Removes a partial value from the centroid calculation with a specified weight factor.
	 *
	 * @param value The value to remove from the centroid calculation.
	 * @param factor The weight factor for this partial removal.
	 */
	void removePartialFromCentroidValue(Double value, double factor);
}
