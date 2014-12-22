/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.math;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;

import prerna.util.ArrayUtilityMethods;

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
	
	public static Double quartileIgnoringNull(Double[] values, final double lowerPercent, final boolean isOrdered) {
		if (values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}

		// remove the null values
		values = (Double[]) ArrayUtilityMethods.removeAllNulls(values);
		
		// Rank order the values if not already ordered
		if(!isOrdered) {
			values = ArrayUtilityMethods.sortDoubleWrapperArr(values);
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
	
	public static double getMinimumValue(final Double[] values) {
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
	
	public static Double getMinimumValueIgnoringNull(final Double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		Double minValue = null;
		for(index = 0; index < size; index++) {
			if(values[index] != null) {
				if(minValue == null) {
					minValue = values[index];
				} else if(minValue > values[index]) {
					minValue = values[index];
				}
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
	
	public static double getMaximumValue(final Double[] values) {
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
	
	public static Double getMaximumValueIgnoringNull(final Double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		Double maxValue = null;
		for(index = 0; index < size; index++) {
			if(values[index] != null) {
				if(maxValue == null) {
					maxValue = values[index];
				} else if(maxValue < values[index]) {
					maxValue = values[index];
				}
			}
		}
		
		return maxValue;
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
	
	public static double getSum(final Double[] values) {
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
	
	public static double getSumIgnoringNull(final Double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double sum = 0.0;
		for(index = 0; index < size; index++) {
			if(values[index] != null) {
				sum += values[index];
			}
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
	
	public static double getAverageIgnoringNull(final Double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		int nonNullSize = 0;
		double sum = 0.0;
		for(index = 0; index < size; index++) {
			if(values[index] != null) {
				sum += values[index];
				nonNullSize++;
			}
		}
		
		return sum/nonNullSize;
	}
	
	public static double getSumIgnoringInfinity(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int size = values.length;
		double sum = 0.0;
		for(index = 0; index < size; index++) {
			double val = values[index];
			if(!Double.isInfinite(val)) {
				sum += values[index];
			}
		}
		
		return sum;
	}
	
	public static double getAverageIgnoringInfinity(final double[] values) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int index;
		int counter = 0;
		int size = values.length;
		double sum = 0.0;
		for(index = 0; index < size; index++) {
			double val = values[index];
			if(!Double.isInfinite(val)) {
				sum += values[index];
				counter++;
			}
		}
		
		return sum/counter;
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
	
	public static double getSampleStandardDeviationIgnoringNull(final Double[] values) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		double avg = getAverageIgnoringNull(values);
		int index;
		int size = values.length;
		int nonNullSize = 0;
		double stdev = 0;
		for(index = 1; index < size; index++) {
			if(values[index] != null) {
				stdev += Math.pow(values[index] - avg,2);
				nonNullSize++;
			}
		}
		
		return Math.pow(stdev/(nonNullSize - 1), 0.5);
	}
	
	public static double getSampleStandardDeviationIgnoringInfinity(final double[] values) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		double avg = getAverageIgnoringInfinity(values);
		int index;
		int counter = 0;
		int size = values.length;
		double stdev = 0;
		for(index = 0; index < size; index++) {
			double val = values[index];
			if(!Double.isInfinite(val)) {
				stdev += Math.pow(values[index] - avg,2);
				counter++;
			}
		}
		
		return Math.pow(stdev/(counter - 1), 0.5);
	}
	
	public static double getMedian(final double[] values, boolean isSorted) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isSorted) {
			Arrays.sort(values);
		}
		
		int middle = values.length/2;
	    if (values.length % 2 == 1) {
	        return values[middle];
	    } else {
	        return (values[middle-1] + values[middle]) / 2.0;
	    }
		
	}
	
	public static double getSkewness(final double[] values, boolean isSorted) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		int numValues = values.length;
		
		double mean = getAverage(values);
		double stdev = getSampleStandardDeviation(values);
		
		int i;
		double skewness = 0;
		for(i = 0; i < numValues; i++) {
			skewness += Math.pow( (values[i] - mean)/stdev, 3.0);
		}
		double coefficient = (double) numValues/ ( (numValues - 1) * (numValues - 2) );
		
		return coefficient * skewness; 
		
	}
	
	public static double getSkewnessIgnoringNull(Double[] values, boolean isSorted) {
		if( values == null || values.length < 1) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isSorted){
			values = ArrayUtilityMethods.sortDoubleWrapperArr(values);
		}
		
		int numValues = values.length;
		
		int index;
		int nonNullSize = 0;
		double sum = 0.0;
		for(index = 0; index < numValues; index++) {
			if(values[index] != null) {
				sum += values[index];
				nonNullSize++;
			}
		}
		
		double mean = sum/nonNullSize;
		double stdev = getSampleStandardDeviationIgnoringNull(values);
		
		int i;
		double skewness = 0;
		for(i = 0; i < numValues; i++) {
			if(values[i] != null) {
				skewness += Math.pow( (values[i] - mean)/stdev, 3.0);
			}
		}
		double coefficient = (double) nonNullSize/ ( (nonNullSize - 1) * (nonNullSize - 2) );
		
		return coefficient * skewness; 
		
	}
	
	public static double[] calculateZScores(final double[] values, final boolean isOrdered) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		double[] newValues = values.clone();
		
		if(!isOrdered){
			Arrays.sort(newValues);
		}
		
		int numValues = values.length;
		double avg = getAverage(newValues);
		double stdev = getSampleStandardDeviation(newValues);

		double[] zScore = new double[numValues];
		int i;
		for(i = 0; i < numValues; i++) {
			zScore[i] = (values[i] - avg)/stdev;
		}
		
		return zScore;
	}
	
	public static double[] calculateZScoresIgnoringInfinity(final double[] values, final boolean isOrdered) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		double[] newValues = values.clone();
		
		if(!isOrdered){
			Arrays.sort(newValues);
		}
		
		int numValues = values.length;
		double avg = getAverageIgnoringInfinity(newValues);
		double stdev = getSampleStandardDeviationIgnoringInfinity(newValues);

		double[] zScore = new double[numValues];
		int i;
		for(i = 0; i < numValues; i++) {
			if(Double.isInfinite(values[i])) {
				zScore[i] = Double.NaN;
			} else {
				if(stdev == 0) {
					zScore[i] = 0;
				} else {
					zScore[i] = (values[i] - avg)/stdev;
				}
			}
		}
		
		return zScore;
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

		double avg = getAverage(values);
		double stdev = getSampleStandardDeviation(values);

		double minZScore = (minVal - avg)/stdev;
		double maxZScore = (maxVal - avg)/stdev;

		int index;
		int start = (int) Math.ceil(minZScore);
		int end = (int) Math.floor(maxZScore);
		
		if(start == end) {
			return new double[]{start};
		}
		
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
	
	public static double[] calculateZScoreRange(Double[] values, final boolean isOrdered) {
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isOrdered){
			values = ArrayUtilityMethods.sortDoubleWrapperArr(values);
		}
		
		double minVal = values[0];
		double maxVal = values[values.length - 1];

		double avg = getAverageIgnoringNull(values);
		double stdev = getSampleStandardDeviationIgnoringNull(values);

		double minZScore = (minVal - avg)/stdev;
		double maxZScore = (maxVal - avg)/stdev;

		int index;
		int start = (int) Math.ceil(minZScore);
		int end = (int) Math.floor(maxZScore);
		
		if(start == end) {
			return new double[]{start};
		}
		
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
	
	public static String[] getZScoreRangeAsStringIgnoringNull(Double[] values, final boolean isOrdered) {
		NumberFormat formatter = new DecimalFormat("#.##");
		if( values == null || values.length == 0) {
			throw new IllegalArgumentException(ILLEGAL_ARGS_ERR);
		}
		
		if(!isOrdered){
			values = ArrayUtilityMethods.sortDoubleWrapperArr(values);
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

		// if only one value, then entropy is 0
		if(values.length == 1) {
			return 0;
		}
		if(ArrayUtilityMethods.removeAllZeroValues(values).length == 1) {
			return 0;
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
