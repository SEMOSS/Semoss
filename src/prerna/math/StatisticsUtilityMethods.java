/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.math;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
				skewness += Math.pow( (values[i] - mean), 3.0);
			}
		}
		double coefficient = (double) nonNullSize/ ( (nonNullSize - 1) * (nonNullSize - 2) );
		
		return coefficient * skewness / Math.pow(stdev, 3); 
		
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
	
	//Calculates entropy from an array of counts
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
				double prob = val / sum;
				entropy += prob * logBase2(prob);
			}
		}
		
		entropy *= -1;
		
		return entropy;
	}
	
	public static double calculateEntropyDensity(final int[] values) {
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

		int uniqueVals = values.length;

		double entropy = calculateEntropy(values);
		double entropyDensity = entropy / uniqueVals;
		
		return entropyDensity;
	}
	
	/**
	 * Generate the log base 2 of a given input
	 * @param x		The value to take the log base 2 off
	 * @return		The log base 2 of the value inputed
	 */
	public static double logBase2(final double x) {
		return Math.log(x) / Math.log(2);
	}
	
	public static double[] calculatePercentDiff(final double[] values1, final double[] values2) {

		int i;
		int length = values1.length;
		double[] percDiffArr = new double[length];

		for(i=0; i<length; i++) {
			if(values1[i] == 0)
				percDiffArr[i] = Double.NaN;
			else
				percDiffArr[i] = (values2[i] / values1[i] - 1) * 100;

		}
		
		return percDiffArr;
	}
	
	public static boolean areValuesUniformlyDistributed(final int[] valueArr, final int p, final int N, final int m, final double alpha) {
		
		double totalKSStat = 0.0;
		int numValues = valueArr.length;
		if(numValues < N) {
			System.out.println("\nERROR: N is less than the number of values in the dataset. Please increase N.");
		}
			
		for(int i = 0; i < p; i++) {
			int startIndex = (int)(Math.random() * (numValues - N));
			int[] randomStartValueArr = Arrays.copyOfRange(valueArr,startIndex,startIndex + N);
			Arrays.sort(randomStartValueArr);
			double ksStat = calculateAverageKSStat(randomStartValueArr,m);
			totalKSStat += ksStat;

			System.out.println("Starting at index " + startIndex + " the average KSStat for all subsets is " + ksStat);
		
		}
		
		double averageKSStat = totalKSStat / p;
		System.out.println("Overall KSStat for randomly generated starts is " + averageKSStat);
		
		if(alpha == 0.05) {
			if(averageKSStat < 0.40925) {
				return true;
			}else {
				return false;
			}
		}else {
			System.out.println("Alpha is not valid. Please enter either 0.05.");
			return false;
		}
	}
	
	//m is number of values in the set
	public static double calculateAverageKSStat(final int[] valueArr, final int m) {
		
		double totalKSStat = 0.0;
		int numValues = valueArr.length;
		int numSets = (int)Math.ceil(numValues / m * 1.0);
		for(int i = 0; i < numSets; i++){
			int[] subsetValueArr = Arrays.copyOfRange(valueArr,i*m,(i+1)*m);
			totalKSStat += calculateKSStat(subsetValueArr);
		}
		
		return totalKSStat / numSets;
	}
	
	public static double calculateKSStat(final int[] valueArr) {

		// Normalize all values
		int numValues = valueArr.length;
		double minValue = valueArr[0];
		double maxValue = valueArr[numValues - 1];
		if(minValue == maxValue) {
			return 1.0 * Math.sqrt(numValues);
		}
		double[] normalizedValueArr = new double[numValues];
		for(int i = 0; i < numValues; i++) {
			normalizedValueArr[i] = (valueArr[i] - minValue) / (maxValue - minValue);
		}
		
		double maxDistance = 0;
		for(int i = 0; i < numValues; i++) {
			double distance = Math.abs(normalizedValueArr[i] - (i / (numValues - 1.0)));
			if(distance > maxDistance) {
				maxDistance = distance;
			}
		}
		
		return maxDistance * Math.sqrt(numValues);
		
	}
	
}
