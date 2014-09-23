package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;

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
		manipulateValues();
	}
	
	//TODO: get to work with nulls/missing data
	private void manipulateValues() {
		int counter = 0;
		
		int numProps = queryData.get(0).length;
		data = new Object[numProps-1][queryData.size()];
		isCategorical = new boolean[numProps];
		Object[][] trackType = new Object[numProps][queryData.size()];
		
		int i;
		int size = queryData.size();
		for(i = 0; i < size; i++) {
			Object[] dataRow = queryData.get(i);
			int j;
			for(j = 1; j < numProps - 1; j++) {
				data[j][counter] = dataRow[j];
				trackType[j][counter] = processType(dataRow[j].toString());
			}
			counter++;
		}
		
		for(i = 1; i < numProps - 1; i++) {
			int j;
			int stringCounter = 0;
			int doubleCounter = 0;
			for(j = 0; j < counter; j++) {
				if(trackType[i][j].toString().equals("STRING")) {
					stringCounter++;
				} else {
					doubleCounter++;
				}
			}
			if(stringCounter > doubleCounter) {
				isCategorical[i] = true;
			}
		}
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
			System.out.println("Bin size: " + numBins);
			int[] values = new int[numBins];
			for(j = 0; j < numBins; j++) {
				values[j] = (int) binData[j].get("y");
				System.out.println("Bin " + j + ": " + values[j]);
			}
			
			double num = StatisticsUtilityMethods.calculateEntropyDensity(values);
			if(Double.isNaN(num)){
				System.out.println("Error!");
			}
			entropyDensityArray[i-1] = StatisticsUtilityMethods.calculateEntropyDensity(values);
			
			System.out.println("Entropy:" + StatisticsUtilityMethods.calculateEntropyDensity(values));
		}
		
		return entropyDensityArray;
	}
	
	/**
	 * Determines the type of a given value
	 * @param s		The value to determine the type off
	 * @return		The type of the value
	 */
	private static String processType(String s) {
		
		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return ("DOUBLE");
		}

		// will analyze date types as numerical data
		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}
		if(isLongDate){
			return ("DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("mm/dd/yyyy", Locale.US);
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}
		if(isSimpleDate){
			return ("SIMPLEDATE");
		}

		return ("STRING");
	}
	
}
