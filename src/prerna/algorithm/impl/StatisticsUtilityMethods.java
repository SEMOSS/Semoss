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

}
