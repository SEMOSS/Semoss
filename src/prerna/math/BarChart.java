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
	private String[] uniqueValues;
	
	private double[] numericalValuesSorted;
	private double[] numericalValuesUnsorted;
	
	private Double[] wrapperNumericalValuesSorted;
	private Double[] wrapperNumericalValuesUnsorted;
	
	private Hashtable<String, Object>[] retHashForJSON;
	private String[] assignmentForEachObject;
	private String[] numericalBinOrder;
	
	private String numericalLabel = "Distribution";
	private String categoricalLabel = "Frequency";
	
	public BarChart(String[] values) {
		this.stringValues = values;
		this.numericalValuesSorted = null;
		assignmentForEachObject = new String[values.length];
		this.uniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
		
		retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues, "?", true);
	}
	
	public BarChart(String[] values, String categoricalLabel) {
		this.stringValues = values;
		this.numericalValuesSorted = null;
		assignmentForEachObject = new String[values.length];
		this.uniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
		this.categoricalLabel = categoricalLabel;

		retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues, "?", true);
	}
	
	public BarChart(double[] values) {
		this.numericalValuesUnsorted = values;
		double[] sortedValues = new double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		Arrays.sort(sortedValues);
		this.numericalValuesSorted = sortedValues;
		
		this.stringValues = null;
		this.uniqueValues = null;
		assignmentForEachObject = new String[values.length];
		// stringValues becomes not null when process finds not enough unique values to make bins and decides to process values as individual strings
		
		retHashForJSON = calculateNumericBins(numericalValuesSorted, numericalValuesUnsorted);
	}
	
	public BarChart(double[] values, String numericalLabel) {
		this.numericalValuesUnsorted = values;
		double[] sortedValues = new double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		Arrays.sort(sortedValues);
		this.numericalValuesSorted = sortedValues;
		
		this.stringValues = null;
		this.uniqueValues = null;
		assignmentForEachObject = new String[values.length];
		this.numericalLabel = numericalLabel;
		
		retHashForJSON = calculateNumericBins(numericalValuesSorted, numericalValuesUnsorted);
	}
	
	public BarChart(Double[] values) {
		this.wrapperNumericalValuesUnsorted = values;
		Double[] sortedValues = new Double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		this.wrapperNumericalValuesSorted = ArrayUtilityMethods.sortDoubleWrapperArr(sortedValues);
		
		this.stringValues = null;
		this.uniqueValues = null;
		assignmentForEachObject = new String[values.length];
		// stringValues becomes not null when process finds not enough unique values to make bins and decides to process values as individual strings
		retHashForJSON = calculateNumericBins(wrapperNumericalValuesSorted, wrapperNumericalValuesUnsorted);
	}
	
	public BarChart(Double[] values, String numericalLabel) {
		this.wrapperNumericalValuesUnsorted = values;
		Double[] sortedValues = new Double[values.length];
		System.arraycopy(values, 0, sortedValues, 0, sortedValues.length);
		this.wrapperNumericalValuesSorted = ArrayUtilityMethods.sortDoubleWrapperArr(sortedValues);
		
		this.stringValues = null;
		this.uniqueValues = null;
		assignmentForEachObject = new String[values.length];
		this.numericalLabel = numericalLabel;

		retHashForJSON = calculateNumericBins(wrapperNumericalValuesSorted, wrapperNumericalValuesUnsorted);
	}
	
	@SuppressWarnings("unchecked")
	private Hashtable<String, Object>[] calculateCategoricalBins(String[] values, String[] uniqueValues, String missingDataSymbol, boolean orderGaussian) {
		int numOccurrences = values.length;
		int uniqueSize = uniqueValues.length;
		int[] uniqueCounts = new int[uniqueSize];
		Hashtable<String, Object>[] retBins = new Hashtable[uniqueSize];
		// find counts for unique variables
		int i;
		for(i = 0; i < numOccurrences; i++) {
			// each instance gets assigned its value
			String val = values[i];
			if(val == null) {
				assignmentForEachObject[i] = missingDataSymbol;
			} else {
				assignmentForEachObject[i] = values[i];
				INNER : for(int j = 0; j < uniqueSize; j++) {
					if(uniqueValues[j].equals(val)) {
						uniqueCounts[j]++;
						break INNER;
					}
				}
			}
		}
		// sort the unique counts from smallest to largest
		for(i = 0; i < uniqueSize - 1; i++) {
			int j;
			// if first index is larger than second index, switch positions in both count and name arrays
			for(j = i+1; j < uniqueSize; j++) {
				if(uniqueCounts[i] > uniqueCounts[j]) {
					int largerVal = uniqueCounts[i];
					int smallerVal = uniqueCounts[j];
					String largerPropName = uniqueValues[i];
					String smallerPropName = uniqueValues[j];
					uniqueCounts[j] = largerVal;
					uniqueCounts[i] = smallerVal;
					uniqueValues[j] = largerPropName;
					uniqueValues[i] = smallerPropName;
				}
			}
		}
		String[] sortedValues = uniqueValues;
		int[] sortedCounts = uniqueCounts;
		
		if(orderGaussian) {
			// order the values to look the normal distribution
			sortedValues = new String[uniqueSize];
			sortedCounts = new int[uniqueSize];
			int center = (int) Math.ceil(uniqueSize / 2.0);
			int sgn;
			for(i = 1, sgn = -1; i <= uniqueSize; i++, sgn *= -1) {
				sortedCounts[center - 1 + (sgn*i/2)] = uniqueCounts[uniqueSize - i];
				sortedValues[center - 1 + (sgn*i/2)] = uniqueValues[uniqueSize - i];
			}
		}
		
		for(i = 0; i < uniqueSize; i++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", categoricalLabel);
			innerHash.put("y0", "0");
			innerHash.put("x", sortedValues[i]);
			innerHash.put("y", sortedCounts[i]);
			retBins[i] = innerHash;
		}
		return retBins;
	}
	
	private Hashtable<String, Object>[] calculateNumericBins(double[] numValues, double[] unsortedValues) {
		NumberFormat formatter = null;
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = Math.abs(max - min);
		if(range <= 1 & min > .01) {
			formatter = new DecimalFormat("#.00");
		} else if(range <= 1) {
			String pattern = "#.";
			String text = Double.toString(Math.abs(range));
			int integerPlaces = text.indexOf('.');
			int decimalPlaces = text.length() - integerPlaces - 1;
			for(int i = 0; i < decimalPlaces + 1; i++) {
				pattern = pattern.concat("0");
			}
			pattern = pattern.concat("E0");
			formatter = new DecimalFormat(pattern);
		} else {
			String pattern = "0.";
			int decimalPlaces = (int) Math.round(Math.log10(range)) + 1;
			for(int i = 0; i < decimalPlaces; i++) {
				pattern = pattern.concat("0");
			}
			pattern = pattern.concat("E0");
			formatter = new DecimalFormat(pattern);
		}
		
		double skewness = StatisticsUtilityMethods.getSkewness(numValues, true);
		int numUniqueValues = ArrayUtilityMethods.getUniqueArray(numValues).length;
		
		if(numUniqueValues < 10 && (Double.isNaN(skewness) || skewness < 1) ) // skewness is NaN when all values are the same
		{
			this.stringValues = ArrayUtilityMethods.convertDoubleArrToStringArr(numValues);
			// values in order since the methods in ArrayUtilityMethods both conserve order
			String[] nonNullValues = ArrayUtilityMethods.getUniqueArray(stringValues);
			numericalBinOrder = nonNullValues;
			this.uniqueValues = numericalBinOrder;
			this.numericalValuesSorted = null;
			this.numericalValuesUnsorted = null;
			return retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues, "NaN", false);
		} else if(numUniqueValues < 10 && skewness > 1 || skewness > 2) {
			return calculateLogDistributedBins(numValues, unsortedValues, formatter);
		} else {
			return calculateFreedmanDiaconisBins(numValues, unsortedValues, formatter);
		}
	}
	
	public Hashtable<String, Object>[] calculateLogDistributedBins(double[] numValues, double[] unsortedValues, NumberFormat formatter) {
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

		return allocateValuesToBin(numBins, binSize, logValuesUnsorted, logValues, formatter);
	}
	
	public Hashtable<String, Object>[] calculateFreedmanDiaconisBins(double[] numValues, double[] unsortedValues, NumberFormat formatter){
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = max - min;
		double iqr = StatisticsUtilityMethods.quartile(numValues, 75, true) - StatisticsUtilityMethods.quartile(numValues, 25, true);
		if(iqr == 0) {
			return calculateLogDistributedBins(numValues, unsortedValues, formatter);
		}
		double binSize = 2 * iqr * Math.pow(numOccurances, -1.0/3.0);
		int numBins = (int) Math.ceil(range/binSize);
		
		return allocateValuesToBin(numBins, binSize, unsortedValues, numValues, formatter);
	}
	
	private Hashtable<String, Object>[] allocateValuesToBin(int numBins, double binSize, double[] unsortedValues, double[] numValues, NumberFormat formatter) {
		int numOccurances = numValues.length;
		double min = numValues[0];
		
		Hashtable<String, Object>[] retBins = new Hashtable[numBins];
		numericalBinOrder = new String[numBins];
		int i = 0;
		double start = min;
		double end = min + binSize;
		// create all bins
		for(;i < numBins; i++) {
			String bin = formatter.format(start) + "  -  " + formatter.format(end);
			numericalBinOrder[i] = bin;

			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", numericalLabel);
			innerHash.put("y0", "0");
			innerHash.put("x", bin);
			retBins[i] = innerHash;
			
			start += binSize;
			end += binSize;
		}
		// determine where each instance belongs
		int[] counterArr = new int[numBins];
		int j;
		for(i = 0; i < numOccurances; i++) {
			double val = unsortedValues[i];
			for(j = 0; j < numBins; j++) {
				String bin = numericalBinOrder[j];
				String[] binSplit = bin.split(" - ");
				double binMin = Double.parseDouble(binSplit[0].trim());
				double binMax = Double.parseDouble(binSplit[1].trim());
				if(binMin <= val && binMax >= val)
				{
					counterArr[j]++;
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
					counterArr[0]++;
					assignmentForEachObject[i] = numericalBinOrder[0];
				} else {
					String maxBin = numericalBinOrder[numBins - 1];
					binSplit = maxBin.split(" - ");
					double binMax = Double.parseDouble(binSplit[0].trim());
					if(val > binMax) {
						counterArr[numBins - 1]++;
						assignmentForEachObject[i] = numericalBinOrder[numBins - 1];
					} else {
						LOGGER.info("ERROR: BINS DO NOT CAPTURE VALUE: " + val);
					}
				}
			}
		}
		for(i = 0; i < numBins; i++) {
			Hashtable<String, Object> innerHash = retBins[i];
			innerHash.put("y", counterArr[i]);
		}
		
		return retBins;
	}
	
	private Hashtable<String, Object>[] calculateNumericBins(Double[] numValues, Double[] unsortedValues) {
		NumberFormat formatter = null;
		Double min = StatisticsUtilityMethods.getMinimumValueIgnoringNull(numValues);
		Double max = StatisticsUtilityMethods.getMaximumValueIgnoringNull(numValues);
		//TODO: figure out what to do when entire values array is null
		double range = Math.abs(max - min);
		if(range <= 1 & min > .01) {
			formatter = new DecimalFormat("#.00");
		} else if(range <= 1) {
			String pattern = "#.";
			String text = Double.toString(Math.abs(range));
			int integerPlaces = text.indexOf('.');
			int decimalPlaces = text.length() - integerPlaces - 1;
			for(int i = 0; i < decimalPlaces + 1; i++) {
				pattern = pattern.concat("0");
			}
			pattern = pattern.concat("E0");
			formatter = new DecimalFormat(pattern);
		} else {
			String pattern = "0.";
			int decimalPlaces = (int) Math.round(Math.log10(range)) + 1;
			for(int i = 0; i < decimalPlaces; i++) {
				pattern = pattern.concat("0");
			}
			pattern = pattern.concat("E0");
			formatter = new DecimalFormat(pattern);
		}
		
		Double skewness = StatisticsUtilityMethods.getSkewnessIgnoringNull(numValues, true);
		int numUniqueValues = ArrayUtilityMethods.getUniqueArrayIgnoringNull(numValues).length;
		
		if(numUniqueValues < 10 && (Double.isNaN(skewness) || skewness < 1) ) // skewness is NaN when all values are the same
		{
			this.stringValues = ArrayUtilityMethods.convertDoubleWrapperArrToStringArr(numValues);
			// values in order since the methods in ArrayUtilityMethods both conserve order
			String[] nonNullValues = (String[]) ArrayUtilityMethods.removeAllNulls(stringValues);
			numericalBinOrder = ArrayUtilityMethods.getUniqueArray(nonNullValues);
			this.uniqueValues = numericalBinOrder;
			this.numericalValuesSorted = null;
			this.numericalValuesUnsorted = null;
			return retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues, "NaN", false);
		} else if(numUniqueValues < 10 && skewness > 1 || skewness > 2) {
			return calculateLogDistributedBins(numValues, unsortedValues, formatter);
		} else {
			return calculateFreedmanDiaconisBins(numValues, unsortedValues, formatter);
		}
	}
	
	public Hashtable<String, Object>[] calculateLogDistributedBins(Double[] numValues, Double[] unsortedValues, NumberFormat formatter) {
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
		
		Double min = StatisticsUtilityMethods.getMinimumValueIgnoringNull(logValues);
		Double max = StatisticsUtilityMethods.getMaximumValueIgnoringNull(logValues);
		//TODO: checks for when min/max are null/the same value
		double range = max - min;
		double binSize = Math.log10(1+max)/Math.pow(numOccurances, (double) 1/3);
		int numBins = (int) Math.ceil(range/binSize);

		return allocateValuesToBin(min, numBins, binSize, logValuesUnsorted, logValues, formatter);
	}
		
	public Hashtable<String, Object>[] calculateFreedmanDiaconisBins(Double[] numValues, Double[] unsortedValues, NumberFormat formatter){
		int numOccurances = numValues.length;
		Double min = StatisticsUtilityMethods.getMinimumValueIgnoringNull(numValues);
		Double max = StatisticsUtilityMethods.getMaximumValueIgnoringNull(numValues);
		//TODO: checks for when min/max are null/the same value
		double range = max - min;
		double iqr = StatisticsUtilityMethods.quartileIgnoringNull(numValues, 75, true) - StatisticsUtilityMethods.quartileIgnoringNull(numValues, 25, true);
		if(iqr == 0) {
			return calculateLogDistributedBins(numValues, unsortedValues, formatter);
		}
		double binSize = 2 * iqr * Math.pow(numOccurances, -1.0/3.0);
		int numBins = (int) Math.ceil(range/binSize);
		
		return allocateValuesToBin(min, numBins, binSize, unsortedValues, numValues, formatter);
	}
	
	
	private Hashtable<String, Object>[] allocateValuesToBin(Double min, int numBins, double binSize, Double[] unsortedValues, Double[] numValues, NumberFormat formatter) {
		int numOccurances = numValues.length;
		
		Hashtable<String, Object>[] retBins = new Hashtable[numBins];
		numericalBinOrder = new String[numBins];
		int i = 0;
		double start = min;
		double end = min + binSize;
		// create all bins
		for(;i < numBins; i++) {
			String bin = formatter.format(start) + "  -  " + formatter.format(end);
			numericalBinOrder[i] = bin;

			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", numericalLabel);
			innerHash.put("y0", "0");
			innerHash.put("x", bin);
			retBins[i] = innerHash;
			
			start += binSize;
			end += binSize;
		}
		// determine where each instance belongs
		int[] counterArr = new int[numBins];
		int j;
		for(i = 0; i < numOccurances; i++) {
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
						counterArr[j]++;
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
						counterArr[0]++;
						assignmentForEachObject[i] = numericalBinOrder[0];
					} else {
						String maxBin = numericalBinOrder[numBins - 1];
						binSplit = maxBin.split(" - ");
						double binMax = Double.parseDouble(binSplit[0].trim());
						if(val > binMax) {
							counterArr[numBins - 1]++;
							assignmentForEachObject[i] = numericalBinOrder[numBins - 1];
						} else {
							LOGGER.info("ERROR: BINS DO NOT CAPTURE VALUE: " + val);
						}
					}
				}
			}
		}
		for(i = 0; i < numBins; i++) {
			Hashtable<String, Object> innerHash = retBins[i];
			innerHash.put("y", counterArr[i]);
		}
		
		return retBins;
	}
		
	
	public Hashtable<String, Object>[] getRetHashForJSON() {
		return retHashForJSON;
	}
	
	public String[] getAssignmentForEachObject() {
		return assignmentForEachObject;
	}
	
	public String[] getNumericalBinOrder() {
		return numericalBinOrder;
	}
}
