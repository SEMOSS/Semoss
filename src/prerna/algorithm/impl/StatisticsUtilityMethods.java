package prerna.algorithm.impl;

import java.util.Arrays;

public class StatisticsUtilityMethods {

	public static double quartile(double[] values, double lowerPercent, boolean ordered) {
		if (values == null || values.length == 0) {
			throw new IllegalArgumentException("The data array either is null or does not contain any data.");
		}

		// Rank order the values if not already ordered
		if(!ordered) {
			Arrays.sort(values);
		}

		int n = (int) Math.floor(values.length * lowerPercent / 100);
		return values[n];
	}
	
	public static double getMinimumValue(double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException("The data array either is null or does not contain any data.");
		}
		
		int i;
		int size = values.length;
		double minValue = values[0];
		for(i = 1; i < size; i++) {
			if(minValue > values[i]) {
				minValue = values[i];
			}
		}
		
		return minValue;
	}
	
	public static double getMaximumValue(double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException("The data array either is null or does not contain any data.");
		}
		
		int i;
		int size = values.length;
		double maxValue = values[0];
		for(i = 1; i < size; i++) {
			if(maxValue < values[i]) {
				maxValue = values[i];
			}
		}
		
		return maxValue;
	}
	
	public static double getAverage(double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException("The data array either is null or does not contain any data.");
		}
		
		int i;
		int size = values.length;
		double sum = values[0];
		for(i = 1; i < size; i++) {
			sum += values[i];
		}
		
		return sum/size;
	}
	
	public static double getSampleStandardDeviation(double[] values) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException("The data array either is null or does not contain any data.");
		}
		
		double avg = getAverage(values);
		int i;
		int size = values.length;
		double stdev = Math.pow((values[0] - avg),2);
		for(i = 1; i < size; i++) {
			stdev += Math.pow((values[i] - avg),2);
		}
		
		return stdev/(size - 1);
	}

}
