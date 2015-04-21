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
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

public class BarChart {

	private static final Logger LOGGER = LogManager.getLogger(BarChart.class.getName());
	
	private String[] stringValues;
	private int[] stringCount;
	
	private String[] stringUniqueValues;
	private int[] stringUniqueCounts;
	
	private double[] numericalValuesSorted;
	private double[] numericalValuesUnsorted;
	
	private Double[] wrapperNumericalValuesSorted;
	private Double[] wrapperNumericalValuesUnsorted;
	
	private String[] assignmentForEachObject;
	private int[] numericBinCount;

	private String[] numericalBinOrder;
	private int[] numericBinCounterArr;
	
	private Hashtable<String, Object>[] retHashForJSON;

	private String numericalLabel = "Distribution";
	private String categoricalLabel = "Frequency";
	
	private boolean useCategoricalForNumericInput;
	
	public BarChart(String[] values) {
		this.stringValues = values;
		this.numericalValuesSorted = null;
		assignmentForEachObject = new String[values.length];
		this.stringUniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
	}
	
	public BarChart(double[] values) {
		this.numericalValuesUnsorted = values;
		double[] sortedValues = new double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		Arrays.sort(sortedValues);
		this.numericalValuesSorted = sortedValues;
		
		this.stringValues = null;
		this.stringUniqueValues = null;
		assignmentForEachObject = new String[values.length];
		// stringValues becomes not null when process finds not enough unique values to make bins and decides to process values as individual strings
		
		calculateNumericBins(numericalValuesSorted, numericalValuesUnsorted);
	}
	
	public BarChart(double[] values, String numericalLabel) {
		this.numericalValuesUnsorted = values;
		double[] sortedValues = new double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		Arrays.sort(sortedValues);
		this.numericalValuesSorted = sortedValues;
		
		this.stringValues = null;
		this.stringUniqueValues = null;
		assignmentForEachObject = new String[values.length];
		this.numericalLabel = numericalLabel;
		
		calculateNumericBins(numericalValuesSorted, numericalValuesUnsorted);
	}
	
	public BarChart(Double[] values) {
		this.wrapperNumericalValuesUnsorted = values;
		Double[] sortedValues = new Double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		this.wrapperNumericalValuesSorted = ArrayUtilityMethods.sortDoubleWrapperArr(sortedValues);
		
		this.stringValues = null;
		this.stringUniqueValues = null;
		assignmentForEachObject = new String[values.length];
		// stringValues becomes not null when process finds not enough unique values to make bins and decides to process values as individual strings
		calculateNumericBins(wrapperNumericalValuesSorted, wrapperNumericalValuesUnsorted);
	}
	
	public BarChart(Double[] values, String numericalLabel) {
		this.wrapperNumericalValuesUnsorted = values;
		Double[] sortedValues = new Double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		this.wrapperNumericalValuesSorted = ArrayUtilityMethods.sortDoubleWrapperArr(sortedValues);
		
		this.stringValues = null;
		this.stringUniqueValues = null;
		assignmentForEachObject = new String[values.length];
		this.numericalLabel = numericalLabel;

		calculateNumericBins(wrapperNumericalValuesSorted, wrapperNumericalValuesUnsorted);
	}
	
	// order[0] = order from min to max
	// order[1] = order as normal distribution
	public void calculateCategoricalBins(String missingDataSymbol, boolean... order) {
		int numOccurrences = stringValues.length;
		stringCount = new int[numOccurrences];
		int uniqueSize = stringUniqueValues.length;
		stringUniqueCounts = new int[uniqueSize];
		
		// find counts for unique variables
		int i;
		for(i = 0; i < numOccurrences; i++) {
			// each instance gets assigned its value
			String val = stringValues[i];
			if(val == null) {
				assignmentForEachObject[i] = missingDataSymbol;
			} else {
				assignmentForEachObject[i] = stringValues[i];
				INNER : for(int j = 0; j < uniqueSize; j++) {
					if(stringUniqueValues[j] != null && stringUniqueValues[j].equals(val)) {
						stringUniqueCounts[j]++;
						break INNER;
					}
				}
			}
		}
		
		for(i = 0; i < numOccurrences; i++) {
			INNER : for(int j = 0; j < uniqueSize; j++) {
				if(stringValues[i] != null && stringValues[i].equals(stringUniqueValues[j])) {
					stringCount[i] = stringUniqueCounts[j];
					break INNER;
				}
			}
		}
		
		if(order[0] || (order.length > 1 && order[1])) { // check size to prevent null point error
			// sort the unique counts from smallest to largest
			for(i = 0; i < uniqueSize - 1; i++) {
				int j;
				// if first index is larger than second index, switch positions in both count and name arrays
				for(j = i+1; j < uniqueSize; j++) {
					if(stringUniqueCounts[i] > stringUniqueCounts[j]) {
						int largerVal = stringUniqueCounts[i];
						int smallerVal = stringUniqueCounts[j];
						String largerPropName = stringUniqueValues[i];
						String smallerPropName = stringUniqueValues[j];
						stringUniqueCounts[j] = largerVal;
						stringUniqueCounts[i] = smallerVal;
						stringUniqueValues[j] = largerPropName;
						stringUniqueValues[i] = smallerPropName;
					}
				}
			}
			
			if(order.length > 1 && order[1]) {
				// order the values to look the normal distribution
				String[] sortedValues = new String[uniqueSize];
				int[] sortedCounts = new int[uniqueSize];
				int center = (int) Math.ceil(uniqueSize / 2.0);
				int sgn;
				for(i = 1, sgn = -1; i <= uniqueSize; i++, sgn *= -1) {
					sortedCounts[center - 1 + (sgn*i/2)] = stringUniqueCounts[uniqueSize - i];
					sortedValues[center - 1 + (sgn*i/2)] = stringUniqueValues[uniqueSize - i];
				}
				stringUniqueValues = sortedValues;
				stringUniqueCounts = sortedCounts;
			}
		}
	}
	
	public Hashtable<String, Object>[] generateJSONHashtableCategorical() {
		String[] nonNullStringUniqueValues = (String[]) ArrayUtilityMethods.removeAllNulls(stringUniqueValues);
		int uniqueSize = nonNullStringUniqueValues.length;
		Hashtable<String, Object>[] retBins = new Hashtable[uniqueSize];
		int i = 0;
		for(; i < uniqueSize; i++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", categoricalLabel);
			innerHash.put("y0", "0");
			innerHash.put("x", nonNullStringUniqueValues[i]);
			innerHash.put("y", stringUniqueCounts[i]);
			retBins[i] = innerHash;
		}
		
		this.retHashForJSON = retBins;
		return retBins;
	}
	
	private void calculateNumericBins(double[] numValues, double[] unsortedValues) {
		
		double skewness = StatisticsUtilityMethods.getSkewness(numValues, true);
		int numUniqueValues = ArrayUtilityMethods.getUniqueArray(numValues).length;
		
		if(numUniqueValues == 1 || (numUniqueValues < 10 && (Double.isNaN(skewness) || skewness <= 1.1)) ) // skewness is NaN when all values are the same
		{
			this.stringValues = ArrayUtilityMethods.convertDoubleArrToStringArr(numValues);
			// values in order since the methods in ArrayUtilityMethods both conserve order
			String[] nonNullValues = ArrayUtilityMethods.getUniqueArray(stringValues);
			numericalBinOrder = nonNullValues;
			this.stringUniqueValues = numericalBinOrder;
			
			this.numericalValuesSorted = null;
			this.numericalValuesUnsorted = null;
			this.useCategoricalForNumericInput = true;
			// need to check if not enough numeric values to generate bins, process as if values are unique
			calculateCategoricalBins("NaN", false);
		} else if(numUniqueValues < 10 && skewness > 1.1 || skewness > 2) {
			calculateLogDistributedBins(numValues, unsortedValues);
		} else {
			calculateFreedmanDiaconisBins(numValues, unsortedValues);
		}
	}
	
	public void calculateLogDistributedBins(double[] numValues, double[] unsortedValues) {
		int numOccurances = numValues.length;
		double[] logValuesUnsorted = new double[numOccurances];
		double[] logValues = new double[numOccurances];
		for(int i = 0; i < numValues.length; i++) {
			if(numValues[i] > 0) {
				logValues[i] = Math.log10(numValues[i] + 1);
			} else {
				logValues[i] = -1 * Math.log10((-1*numValues[i]) + 1);
			}
			if(unsortedValues[i] > 0) {
				logValuesUnsorted[i] = Math.log10(unsortedValues[i] + 1);
			} else {
				logValuesUnsorted[i] = -1 * Math.log10((-1 * unsortedValues[i]) + 1);
			}
		}
		
		double min = logValues[0];
		double max = logValues[numOccurances -1];
		double range = max - min;
		double binSize = Math.log10(1+max)/Math.pow(numOccurances, (double) 1/3);
		int numBins = (int) Math.ceil(range/binSize);

		createBins(numBins, binSize, unsortedValues, numValues, true);
		allocateValuesToBin(numBins, binSize, unsortedValues, numValues);
	}
	
	public void calculateFreedmanDiaconisBins(double[] numValues, double[] unsortedValues){
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = max - min;
		double iqr = StatisticsUtilityMethods.quartile(numValues, 75, true) - StatisticsUtilityMethods.quartile(numValues, 25, true);
		if(iqr == 0) {
			calculateLogDistributedBins(numValues, unsortedValues);
		} else {
			double binSize = 2 * iqr * Math.pow(numOccurances, -1.0/3.0);
			int numBins = (int) Math.ceil(range/binSize);

			createBins(numBins, binSize, unsortedValues, numValues, false);
			allocateValuesToBin(numBins, binSize, unsortedValues, numValues);
		}
	}
	
	private void allocateValuesToBin(int numBins, double binSize, double[] unsortedValues, double[] numValues) {
		int numOccurrences = numValues.length;
		// determine where each instance belongs
		numericBinCount = new int[numOccurrences];
		numericBinCounterArr = new int[numBins];
		int i;
		int j;
		for(i = 0; i < numOccurrences; i++) {
			double val = unsortedValues[i];
			for(j = 0; j < numBins; j++) {
				String bin = numericalBinOrder[j];
				String[] binSplit = bin.split(" - ");
				double binMin = Double.parseDouble(binSplit[0].trim());
				double binMax = Double.parseDouble(binSplit[1].trim());
				if(binMin <= val && binMax >= val)
				{
					numericBinCounterArr[j]++;
					assignmentForEachObject[i] = bin;
					break;
				}
			}
			
			if(assignmentForEachObject[i] == null) {
				// due to rounding
				String minBin = numericalBinOrder[0];
				String[] binSplit = minBin.split(" - ");
				double binMin = Double.parseDouble(binSplit[0].trim());
				if(val < binMin) {
					numericBinCounterArr[0]++;
					assignmentForEachObject[i] = numericalBinOrder[0];
				} else {
					String maxBin = numericalBinOrder[numBins - 1];
					binSplit = maxBin.split(" - ");
					double binMax = Double.parseDouble(binSplit[0].trim());
					if(val > binMax) {
						numericBinCounterArr[numBins - 1]++;
						assignmentForEachObject[i] = numericalBinOrder[numBins - 1];
					} else {
						LOGGER.info("ERROR: BINS DO NOT CAPTURE VALUE: " + val);
					}
				}
			}
		}
		
		for(i = 0; i < numOccurrences; i++) {
			INNER : for(j = 0; j < numBins; j++) {
				if(assignmentForEachObject[i].equals(numericalBinOrder[j])) {
					numericBinCount[i] = numericBinCounterArr[j];
					break INNER;
				}
			}
		}
	}
	
	private void calculateNumericBins(Double[] numValues, Double[] unsortedValues) {		
		Double skewness = StatisticsUtilityMethods.getSkewnessIgnoringNull(numValues, true);
		int numUniqueValues = ArrayUtilityMethods.getUniqueArrayIgnoringNull(numValues).length;
		
		if(numUniqueValues == 1 || (numUniqueValues < 10 && (Double.isNaN(skewness) || skewness <= 1.1) ) ) // skewness is NaN when all values are the same
		{
			this.stringValues = ArrayUtilityMethods.convertDoubleWrapperArrToStringArr(numValues);
			// values in order since the methods in ArrayUtilityMethods both conserve order
			String[] nonNullValues = ArrayUtilityMethods.getUniqueArray(stringValues);
			numericalBinOrder = nonNullValues;
			this.stringUniqueValues = numericalBinOrder;
			
			this.numericalValuesSorted = null;
			this.numericalValuesUnsorted = null;
			this.useCategoricalForNumericInput = true;
			// need to check if not enough numeric values to generate bins, process as if values are unique
			calculateCategoricalBins("NaN", false);
		} else if(numUniqueValues < 10 && skewness > 1.1 || skewness > 2) {
			calculateLogDistributedBins(numValues, unsortedValues);
		} else {
			calculateFreedmanDiaconisBins(numValues, unsortedValues);
		}
	}
	
	public void calculateLogDistributedBins(Double[] numValues, Double[] unsortedValues) {
		int numOccurances = numValues.length;
		Double[] logValuesUnsorted = new Double[numOccurances];
		Double[] logValues = new Double[numOccurances];
		for(int i = 0; i < numValues.length; i++) {
			if(numValues[i] != null) {
				if(numValues[i] > 0) {
					logValues[i] = Math.log10(numValues[i] + 1);
				} else {
					logValues[i] = -1 * Math.log10((-1*numValues[i]) + 1);
				}
			}
			if(unsortedValues[i] != null) {
				if(unsortedValues[i] > 0) {
					logValuesUnsorted[i] = Math.log10(unsortedValues[i] + 1);
				} else {
					logValuesUnsorted[i] = -1 * Math.log10((-1 * unsortedValues[i]) + 1);
				}
			}
		}
		
		Double minLog = StatisticsUtilityMethods.getMinimumValueIgnoringNull(logValues);
		Double maxLog = StatisticsUtilityMethods.getMaximumValueIgnoringNull(logValues);
		double range = maxLog - minLog;
		double binSize = Math.log10(1+maxLog)/Math.pow(numOccurances, (double) 1/3);
		int numBins = (int) Math.ceil(range/binSize);

		createBins(numBins, binSize, unsortedValues, numValues, true);
		allocateValuesToBin(minLog, numBins, binSize, unsortedValues, numValues);
	}
		
	public void calculateFreedmanDiaconisBins(Double[] numValues, Double[] unsortedValues){
		int numOccurances = numValues.length;
		Double min = StatisticsUtilityMethods.getMinimumValueIgnoringNull(numValues);
		Double max = StatisticsUtilityMethods.getMaximumValueIgnoringNull(numValues);
		double range = max - min;
		double iqr = StatisticsUtilityMethods.quartileIgnoringNull(numValues, 75, true) - StatisticsUtilityMethods.quartileIgnoringNull(numValues, 25, true);
		if(iqr == 0) {
			calculateLogDistributedBins(numValues, unsortedValues);
		} else {
			double binSize = 2 * iqr * Math.pow(numOccurances, -1.0/3.0);
			int numBins = (int) Math.ceil(range/binSize);

			createBins(numBins, binSize, unsortedValues, numValues, false);
			allocateValuesToBin(min, numBins, binSize, unsortedValues, numValues);
		}
	}
	
	
	private void allocateValuesToBin(Double min, int numBins, double binSize, Double[] unsortedValues, Double[] numValues) {
		int numOccurrences = numValues.length;
		// determine where each instance belongs
		numericBinCount = new int[numOccurrences];
		numericBinCounterArr = new int[numBins];
		
		int i;
		int j;
		for(i = 0; i < numOccurrences; i++) {
			Double val = unsortedValues[i];
			if(val == null) {
				assignmentForEachObject[i] = "NaN";
			} else {
				for(j = 0; j < numBins; j++) {
					String bin = numericalBinOrder[j];
					String[] binSplit = bin.split(" - ");
					double binMin = Double.parseDouble(binSplit[0].trim());
					double binMax = Double.parseDouble(binSplit[1].trim());
					if(binMin <= val && binMax >= val)
					{
						numericBinCounterArr[j]++;
						assignmentForEachObject[i] = bin;
						break;
					}
				}
				
				if(assignmentForEachObject[i] == null) {
					// due to rounding
					String minBin = numericalBinOrder[0];
					String[] binSplit = minBin.split(" - ");
					double binMin = Double.parseDouble(binSplit[0].trim());
					if(val < binMin) {
						numericBinCounterArr[0]++;
						assignmentForEachObject[i] = numericalBinOrder[0];
					} else {
						String maxBin = numericalBinOrder[numBins - 1];
						binSplit = maxBin.split(" - ");
						double binMax = Double.parseDouble(binSplit[0].trim());
						if(val > binMax) {
							numericBinCounterArr[numBins - 1]++;
							assignmentForEachObject[i] = numericalBinOrder[numBins - 1];
						} else {
							LOGGER.info("ERROR: BINS DO NOT CAPTURE VALUE: " + val);
						}
					}
				}
			}
		}
		
		for(i = 0; i < numOccurrences; i++) {
			INNER : for(j = 0; j < numBins; j++) {
				if(assignmentForEachObject[i].equals(numericalBinOrder[j])) {
					numericBinCount[i] = numericBinCounterArr[j];
					break INNER;
				}
			}
		}
	}
	
	private NumberFormat determineFormatter(double minVal, double maxVal, double binSize) {
		NumberFormat formatter = null;
		
		double range = maxVal - minVal;
		if(binSize < 1) {
			String pattern = "0.";
			while(binSize < 1) {
				binSize *= 10;
				pattern += "0";
			}
			formatter = new DecimalFormat(pattern);
		}else {
			String pattern = "#.";
			while(range > 1) {
				range /= 10;
				binSize /= 10;
				if(binSize < 1)
					pattern += "#";
			}
			pattern += "E0";

			formatter = new DecimalFormat(pattern);		
		}
		
		return formatter;
	}
	
	private void createBins(int numBins, double binSize, double[] unsortedValues, double[] numValues, boolean log) {
		double min = numValues[0];
		double max = numValues[numValues.length-1];
		numericalBinOrder = new String[numBins];
		int i = 0;
		// create all bins
		if(log) {
			double startLog = 0;
			if(min < 0) {
				startLog = Math.log10(-1*min + 1);
			} else {
				startLog = Math.log10(min + 1);
			}
			double endLog = startLog + binSize;
			
			double maxBinSize = Math.pow(10, startLog + binSize * (numBins-1)) - 1;
			NumberFormat formatter = determineFormatter(min, max, maxBinSize);
			
			double start = min;
			double end = Math.pow(10, endLog) - 1;
			
			for(;i < numBins; i++) {	
				String bin = formatter.format(start) + "  -  " + formatter.format(end);
				numericalBinOrder[i] = bin;

				startLog += binSize;
				endLog += binSize;
				
				start = Math.pow(10, startLog) - 1;
				end = Math.pow(10, endLog) - 1;
			}
		} else {
			NumberFormat formatter = determineFormatter(min, max, binSize);
			double start = min;
			double end = min + binSize;
			for(;i < numBins; i++) {	
				String bin = formatter.format(start) + "  -  " + formatter.format(end);
				numericalBinOrder[i] = bin;
				start += binSize;
				end += binSize;
			}
		}
	}
	
	private void createBins(int numBins, double binSize, Double[] unsortedValues, Double[] numValues, boolean log) {
		Double min = StatisticsUtilityMethods.getMinimumValueIgnoringNull(numValues);
		Double max = StatisticsUtilityMethods.getMaximumValueIgnoringNull(numValues);
		numericalBinOrder = new String[numBins];
		int i = 0;
		// create all bins
		if(log) {
			double startLog = 0;
			if(min < 0) {
				startLog = Math.log10(-1*min + 1);
			} else {
				startLog = Math.log10(min + 1);
			}
			double endLog = startLog + binSize;
			
			double maxBinSize = Math.pow(10, startLog + binSize * (numBins-1)) - 1;
			NumberFormat formatter = determineFormatter(min, max, maxBinSize);
			
			double start = min;
			double end = Math.pow(10, endLog) - 1;
			
			for(;i < numBins; i++) {	
				String bin = formatter.format(start) + "  -  " + formatter.format(end);
				numericalBinOrder[i] = bin;

				startLog += binSize;
				endLog += binSize;
				
				start = Math.pow(10, startLog) - 1;
				end = Math.pow(10, endLog) - 1;
			}
		}  else {
			NumberFormat formatter = determineFormatter(min, max, binSize);
			double start = min;
			double end = min + binSize;
			for(;i < numBins; i++) {	
				String bin = formatter.format(start) + "  -  " + formatter.format(end);
				numericalBinOrder[i] = bin;
				start += binSize;
				end += binSize;
			}
		}
	}
	
	public Hashtable<String, Object>[] generateJSONHashtableNumerical() {
		int numBins = numericalBinOrder.length;
		
		Hashtable<String, Object>[] retBins = new Hashtable[numBins];
		int i = 0;
		for(i = 0; i < numBins; i++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", numericalLabel);
			innerHash.put("y0", "0");
			innerHash.put("x", numericalBinOrder[i]);
			innerHash.put("y", numericBinCounterArr[i]);
			
			retBins[i] = innerHash;
		}
		retHashForJSON = retBins;
		return retBins;
	}
	
	public String[] getStringValues() {
		return stringValues;
	}

	public void setStringValues(String[] stringValues) {
		this.stringValues = stringValues;
	}

	public int[] getStringCount() {
		return stringCount;
	}

	public void setStringCount(int[] stringCount) {
		this.stringCount = stringCount;
	}

	public String[] getUniqueValues() {
		return stringUniqueValues;
	}

	public void setUniqueValues(String[] stringUniqueValues) {
		this.stringUniqueValues = stringUniqueValues;
	}

	public double[] getNumericalValuesSorted() {
		return numericalValuesSorted;
	}

	public void setNumericalValuesSorted(double[] numericalValuesSorted) {
		this.numericalValuesSorted = numericalValuesSorted;
	}

	public double[] getNumericalValuesUnsorted() {
		return numericalValuesUnsorted;
	}

	public void setNumericalValuesUnsorted(double[] numericalValuesUnsorted) {
		this.numericalValuesUnsorted = numericalValuesUnsorted;
	}

	public Double[] getWrapperNumericalValuesSorted() {
		return wrapperNumericalValuesSorted;
	}

	public void setWrapperNumericalValuesSorted(
			Double[] wrapperNumericalValuesSorted) {
		this.wrapperNumericalValuesSorted = wrapperNumericalValuesSorted;
	}

	public Double[] getWrapperNumericalValuesUnsorted() {
		return wrapperNumericalValuesUnsorted;
	}

	public void setWrapperNumericalValuesUnsorted(
			Double[] wrapperNumericalValuesUnsorted) {
		this.wrapperNumericalValuesUnsorted = wrapperNumericalValuesUnsorted;
	}

	public String getNumericalLabel() {
		return numericalLabel;
	}

	public void setNumericalLabel(String numericalLabel) {
		this.numericalLabel = numericalLabel;
	}

	public String getCategoricalLabel() {
		return categoricalLabel;
	}

	public void setCategoricalLabel(String categoricalLabel) {
		this.categoricalLabel = categoricalLabel;
	}

	public void setRetHashForJSON(Hashtable<String, Object>[] retHashForJSON) {
		this.retHashForJSON = retHashForJSON;
	}

	public void setAssignmentForEachObject(String[] assignmentForEachObject) {
		this.assignmentForEachObject = assignmentForEachObject;
	}

	public void setNumericalBinOrder(String[] numericalBinOrder) {
		this.numericalBinOrder = numericalBinOrder;
	}

	public Hashtable<String, Object>[] getRetHashForJSON() {
		return retHashForJSON;
	}
	
	public String[] getStringUniqueValues() {
		return stringUniqueValues;
	}

	public void setStringUniqueValues(String[] stringUniqueValues) {
		this.stringUniqueValues = stringUniqueValues;
	}

	public int[] getStringUniqueCounts() {
		return stringUniqueCounts;
	}

	public void setStringUniqueCounts(int[] stringUniqueCounts) {
		this.stringUniqueCounts = stringUniqueCounts;
	}

	public int[] getNumericBinCount() {
		return numericBinCount;
	}

	public void setNumericBinCount(int[] numericBinCount) {
		this.numericBinCount = numericBinCount;
	}

	public int[] getNumericBinCounterArr() {
		return numericBinCounterArr;
	}

	public void setNumericBinCounterArr(int[] numericBinCounterArr) {
		this.numericBinCounterArr = numericBinCounterArr;
	}

	public boolean isUseCategoricalForNumericInput() {
		return useCategoricalForNumericInput;
	}

	public void setUseCategoricalForNumericInput(
			boolean useCategoricalForNumericInput) {
		this.useCategoricalForNumericInput = useCategoricalForNumericInput;
	}

	public String[] getAssignmentForEachObject() {
		return assignmentForEachObject;
	}
	
	public String[] getNumericalBinOrder() {
		return numericalBinOrder;
	}
}
