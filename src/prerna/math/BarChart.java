package prerna.math;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Hashtable;

import prerna.util.ArrayUtilityMethods;

public class BarChart {

	private String[] stringValues;
	private String[] uniqueValues;
	private double[] numericalValues;
	private Hashtable<String, Object>[] retHashForJSON;
	private String[] assignmentForEachObject;
	private String[] numericalBinOrder;
	
	public BarChart(String[] values) {
		this.stringValues = values;
		this.numericalValues = null;
		assignmentForEachObject = null;
		this.uniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
		retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues);
	}
	
	// requires values to be sorted
	public BarChart(double[] values) {
		this.numericalValues = values;
		this.stringValues = null;
		this.uniqueValues = null;
		assignmentForEachObject = new String[values.length];
		// stringValues becomes not null when process finds not enough unique values to make bins and decides to process values as individual strings
		retHashForJSON = calculateNumericBins(numericalValues);
	}
	
	@SuppressWarnings("unchecked")
	private Hashtable<String, Object>[] calculateCategoricalBins(String[] values, String[] uniqueValues) {
		int numOccurrences = values.length;
		int uniqueSize = uniqueValues.length;
		int[] uniqueCounts = new int[uniqueSize];
		Hashtable<String, Object>[] retBins = new Hashtable[uniqueSize];
		// find counts for unique variables
		int i;
		for(i = 0; i < numOccurrences; i++) {
			INNER : for(int j = 0; j < uniqueSize; j++) {
				if(uniqueValues[j].equals(values[i])) {
					uniqueCounts[j]++;
					break INNER;
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
		// order the values to look the normal distribution
		String[] sortedValues = new String[uniqueSize];
		int[] sortedCounts = new int[uniqueSize];
		int center = (int) Math.ceil(uniqueSize / 2.0);
		int sgn;
		for(i = 1, sgn = -1; i <= uniqueSize; i++, sgn *= -1) {
			sortedCounts[center - 1 + (sgn*i/2)] = uniqueCounts[uniqueSize - i];
			sortedValues[center - 1 + (sgn*i/2)] = uniqueValues[uniqueSize - i];
		}
		for(i = 0; i < uniqueSize; i++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", "Frequency");
			innerHash.put("y0", "0");
			innerHash.put("x", sortedValues[i]);
			innerHash.put("y", sortedCounts[i]);
			retBins[i] = innerHash;
		}
		return retBins;
	}

	private Hashtable<String, Object>[] calculateNumericBins(double[] numValues) {
		NumberFormat formatter = null;
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		if(Math.abs(min) >= 0 && Math.abs(max) <= 1) {
			formatter = new DecimalFormat("#.00");
		} else if(Math.abs(min) >= 0 && Math.abs(max) <= 100) {
			formatter = new DecimalFormat("0.00");
		} else if(Math.abs(min) * 10 < Math.abs(max)) {
			formatter = new DecimalFormat("0.00E0");
		} else {
			formatter = new DecimalFormat("0.#E0");
		}
		
		double skewness = StatisticsUtilityMethods.getSkewness(numValues, true);
		int numUniqueValues = ArrayUtilityMethods.getUniqueArray(numValues).length;
		
		if(numUniqueValues < 10 && skewness < 1) {
			String[] numValuesAsString = ArrayUtilityMethods.convertDoubleArrToStringArr(numValues);
			// values in order since the methods in ArrayUtilityMethods both conserve order
			numericalBinOrder = ArrayUtilityMethods.getUniqueArray(numValuesAsString);
			this.stringValues = ArrayUtilityMethods.convertDoubleArrToStringArr(numValues);
			this.uniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
			this.numericalValues = null;
			return retHashForJSON = calculateCategoricalBins(stringValues, uniqueValues);
		} else if(numUniqueValues < 10 && skewness > 1) {
			return calculatePoorlyDistributedBins(numValues, formatter);
		} else {
			return calculateFreedmanDiaconisBins(numValues, formatter);
		}
	}
	
	public Hashtable<String, Object>[] calculatePoorlyDistributedBins(double[] numValues, NumberFormat formatter) {
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = max - min;
		double binSize = range / Math.pow(numOccurances, (double) 1/3);
		int numBins = (int) Math.ceil(range/binSize);
		return allocateValuesToBin(numBins, binSize, numValues, formatter);
	}
	
	public Hashtable<String, Object>[] calculateFreedmanDiaconisBins(double[] numValues, NumberFormat formatter){
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = max - min;
		double iqr = StatisticsUtilityMethods.quartile(numValues, 75, true) - StatisticsUtilityMethods.quartile(numValues, 25, true);
		double binSize = 2 * iqr * Math.pow(numOccurances, -1.0/3.0);
		int numBins = (int) Math.ceil(range/binSize);
		
		return allocateValuesToBin(numBins, binSize, numValues, formatter);
	}
	
	private Hashtable<String, Object>[] allocateValuesToBin(int numBins, double binSize, double[] numValues, NumberFormat formatter) {
		int numOccurances = numValues.length;
		double min = numValues[0];
		
		Hashtable<String, Object>[] retBins = new Hashtable[numBins];
		numericalBinOrder = new String[numBins];
		int i;
		int currBin = 0;
		int counter = 0;
		double start = min;
		double end = min + binSize;
		String bin = formatter.format(start) + "  -  " + formatter.format(end);
		for(i = 0; i < numOccurances; i++) {
			if(numValues[i] >= start && numValues[i] < end){
				assignmentForEachObject[i] = bin;
				numericalBinOrder[currBin] = bin;
				counter++;
			} else {
				do {
					Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
					innerHash.put("seriesName", "Distribution");
					innerHash.put("y0", "0");
					bin = formatter.format(start) + "  -  " + formatter.format(end);
					innerHash.put("x", bin);
					innerHash.put("y", counter);
					retBins[currBin] = innerHash;
					counter = 0;
					currBin++;
					start += binSize;
					end += binSize;
					bin = formatter.format(start) + "  -  " + formatter.format(end);
					assignmentForEachObject[i] = bin;
					numericalBinOrder[currBin] = bin;
				} while(numValues[i] > end); // continue until adding empty bins until value lies within current bin
				counter++; // take into consideration the occurrence that didn't fit in the bin;
			}
			if(i == numOccurances - 1) {
				if(retBins[numBins - 1] == null) {
					Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
					innerHash.put("seriesName", "Distribution");
					innerHash.put("y0", "0");
					bin = formatter.format(start) + "  -  " + formatter.format(end);
					innerHash.put("x", bin);
					innerHash.put("y", counter);
					retBins[currBin] = innerHash;
					assignmentForEachObject[i] = bin;
					numericalBinOrder[currBin] = bin;
				} else {
					//  case when the end point is not included
					Hashtable<String, Object> innerHash = retBins[numBins - 1];
					int currCount = (int) innerHash.get("y");
					innerHash.put("y", currCount+1);
				}
			}
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
