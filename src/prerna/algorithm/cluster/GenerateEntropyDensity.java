package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import prerna.algorithm.impl.AlgorithmDataFormatting;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class GenerateEntropyDensity {
	
	private ArrayList<Object[]> queryData;
	private Object[][] data;
	private double[] entropyDensityArray;
	private boolean[] isCategorical;
	
	public double[] getEntropyDensityArray() {
		return entropyDensityArray;
	}
	
	public GenerateEntropyDensity(ArrayList<Object[]> queryData) {
		this.queryData = queryData;
		AlgorithmDataFormatting formatter = new AlgorithmDataFormatting();
		data = formatter.manipulateValues(queryData);
		isCategorical = formatter.getIsCategorical();
	}
	
	public double[] generateEntropy() {
		int i;
		int size = data.length;
		entropyDensityArray = new double[size - 1];
		
		for(i = 1; i < size; i++) {
			Object[] objDataRow = data[i];
			Hashtable<String, Object>[] binData = null;
			if(isCategorical[i]) {
				String[] dataRow = ArrayUtilityMethods.convertObjArrToStringArr(objDataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			} else {
				double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleArr(objDataRow);
				Arrays.sort(dataRow);
				BarChart chart = new BarChart(dataRow);
				binData = chart.getRetHashForJSON();
			}
			
			int j;
			int numBins = binData.length;
			int[] values = new int[numBins];
			for(j = 0; j < numBins; j++) {
				values[j] = (int) binData[j].get("y");
			}
			
			entropyDensityArray[i-1] = StatisticsUtilityMethods.calculateEntropyDensity(values);
		}
		
		return entropyDensityArray;
	}
	
}
