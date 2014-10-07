package prerna.math;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;

public final class StatisticsUtilityMethods {

	private static final String ILLEGAL_ARGS_ERR = "The data array either is null or does not contain any data.";

	private StatisticsUtilityMethods(){
		
	}
	
	public static double quartile(final double[] values, final double lowerPercent, final boolean isOrdered) {
		if (values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}

		// Rank order the values if not already ordered
		if(!isOrdered) {
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
	
	public static double getSum(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double sum = values[0];
		for(index = 1; index < size; index++) {
			sum += values[index];
		}
		
		return sum;
	}
	
	public static int getMinimumValue(final int[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		int minValue = values[0];
		for(index = 1; index < size; index++) {
			if(minValue > values[index]) {
				minValue = values[index];
			}
		}
		
		return minValue;
	}
	
	public static int getMaximumValue(final int[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		int maxValue = values[0];
		for(index = 1; index < size; index++) {
			if(maxValue < values[index]) {
				maxValue = values[index];
			}
		}
		
		return maxValue;
	}
	
	public static int getSum(final int[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		int sum = values[0];
		for(index = 1; index < size; index++) {
			sum += values[index];
		}
		
		return sum;
	}
	
	public static double getAverage(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int size = values.length;
		double sum = getSum(values);
		
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
	
	public static double[] calculateZScoreRange(final double[] values, final boolean isOrdered) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isOrdered){
			Arrays.sort(values);
		}
		
		double minVal = values[0];
		double maxVal = values[values.length - 1];

		double avg = StatisticsUtilityMethods.getAverage(values);
		double stdev = StatisticsUtilityMethods.getSampleStandardDeviation(values);

		double minZScore = (minVal - avg)/stdev;
		double maxZScore = (maxVal - avg)/stdev;

		int index;
		int start = (int) Math.ceil(minZScore);
		int end = (int) Math.floor(maxZScore);
		if( (start-minZScore)/(maxZScore - minZScore) < 0.05 ) {
			start++;
		}
		if( (maxZScore-end)/(maxZScore - minZScore) < 0.05 ) {
			end--;
		}
		
		double[] zScore = new double[end - start + 3]; //+3 due to minZScore, maxZScore, and including the end value
		zScore[0] = minZScore;
		zScore[zScore.length - 1] = maxZScore;
		int counter = 1;
		for(index = start; index <= end; index++){
			zScore[counter] = index;
			counter++;
		}

		return zScore;
	}
	
	public static String[] getZScoreRangeAsString(final double[] values, final boolean isOrdered) {
		NumberFormat formatter = new DecimalFormat("#.##");
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isOrdered){
			Arrays.sort(values);
		}
		
		double[] zScoreVals = calculateZScoreRange(values, true);
		int i;
		int size = zScoreVals.length;
		String[] zScoreValsAsString = new String[size];
		for(i = 0; i < size; i++) {
			zScoreValsAsString[i] = formatter.format(zScoreVals[i]);
		}
		
		return zScoreValsAsString;
	}
	
	public static double calculateEntropy(final int[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}

		double entropy = 0;
		double sum = getSum(values);
		int index;
		for(index = 0; index < values.length; index++) {
			double val = values[index];
			if(val != 0) {
				entropy += val / sum * logBase2(val / sum);
			}
		}
		
		entropy *= -1;
		
		return entropy;
	}
	
	public static double calculateEntropyDensity(final int[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}

		int uniqueVals = values.length;
		double entropy = calculateEntropy(values);
		entropy *= 1.0/uniqueVals;
		
		return entropy;
	}
	
	/**
	 * Generate the log base 2 of a given input
	 * @param x		The value to take the log base 2 off
	 * @return		The log base 2 of the value inputed
	 */
	public static double logBase2(final double x) {
		return Math.log(x) / Math.log(2);
	}
	
}
