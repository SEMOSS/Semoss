package prerna.algorithm.impl;

import java.util.Arrays;

public final class StatisticsUtilityMethods {

	private static final String ILLEGAL_ARGS_ERR = "The data array either is null or does not contain any data.";

	private StatisticsUtilityMethods(){
		
	}
	
	public static double quartile(final double[] values, final double lowerPercent, final boolean ordered) {
		if (values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}

		// Rank order the values if not already ordered
		if(!ordered) {
			Arrays.sort(values);
		}

		int index = (int) Math.floor(values.length * lowerPercent / 100);
		return values[index];
	}
	
	public static double getMinimumValue(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double minValue = values[0];
		for(index = 1; index < size; index++) {
			if(minValue > values[index]) {
				minValue = values[index];
			}
		}
		
		return minValue;
	}
	
	public static double getMaximumValue(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double maxValue = values[0];
		for(index = 1; index < size; index++) {
			if(maxValue < values[index]) {
				maxValue = values[index];
			}
		}
		
		return maxValue;
	}
	
	public static double getAverage(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double sum = values[0];
		for(index = 1; index < size; index++) {
			sum += values[index];
		}
		
		return sum/size;
	}
	
	public static double getSampleStandardDeviation(final double[] values) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		double avg = getAverage(values);
		int index;
		int size = values.length;
		double stdev = Math.pow(values[0] - avg,2);
		for(index = 1; index < size; index++) {
			stdev += Math.pow(values[index] - avg,2);
		}
		
		return Math.pow(stdev/(size - 1), 0.5);
	}

}
