package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import prerna.algorithm.impl.AlgorithmDataFormatting;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class GenerateEntropyDensity {
	
	private Object[][] data;
	private double[] entropyDensityArray;
	private boolean[] isCategorical;
	private boolean includeLastColumn = false;
	
	public double[] getEntropyDensityArray() {
		return entropyDensityArray;
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData) {
		AlgorithmDataFormatting formatter = new AlgorithmDataFormatting();
		data = formatter.manipulateValues(queryData);
		isCategorical = formatter.getIsCategorical();
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData, boolean includeLastColumn) {
		this.includeLastColumn = includeLastColumn;
		AlgorithmDataFormatting formatter = new AlgorithmDataFormatting();
		formatter.setIncludeLastColumn(includeLastColumn);
		data = formatter.manipulateValues(queryData);
		isCategorical = formatter.getIsCategorical();
	}
	
	public double[] generateEntropy() {
		int i;
		int size = data.length;
		if(includeLastColumn) {
			entropyDensityArray = new double[size];
		}
		else {
			entropyDensityArray = new double[size - 1];
		}
		for(i = 1; i < size; i++) {
			Object[] objDataRow = data[i];
			Hashtable<String, Object>[] binData = null;
			if(isCategorical[i]) {
				String[] dataRow = ArrayUtilityMethods.convertObjArrToStringArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			} else {
				Double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			}
			
			int j;
			int numBins = binData.length;
			if(numBins > 1) {
				int[] values = new int[numBins];
				for(j = 0; j < numBins; j++) {
					values[j] = (int) binData[j].get("y");
				}
				entropyDensityArray[i-1] = StatisticsUtilityMethods.calculateEntropyDensity(values);
			} else { // if all same value (numBins being 1), entropy value is 0
				entropyDensityArray[i-1] = 0;
			}
		}
		
		return entropyDensityArray;
	}
	
}
