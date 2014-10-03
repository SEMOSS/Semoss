package prerna.algorithm.weka.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Locale;

import prerna.math.BarChart;
import prerna.util.ArrayUtilityMethods;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public final class WekaUtilityMethods {

	private WekaUtilityMethods() {
		
	}
	
	// currently only works for clean data - cannot mix strings with doubles for attributes
	public static Instances createInstancesFromQuery(String nameDataSet, ArrayList<Object[]> dataList, String[] names, int attributeIndex) {
		int numInstances = dataList.size();	
		
		int i;
		int j;
		int numAttr = names.length;
		boolean[] isCategorical = new boolean[numAttr];
		HashSet<String>[] nominalValues = new HashSet[numAttr];
		Double[][] numericValues = new Double[numAttr][numInstances];
		for(i = 0; i < numAttr; i++) {
			if(nominalValues[i] == null) {
				nominalValues[i] = new HashSet<String>();
			}

			int numNominal = 0;
			int numNumeric = 0;
			for(j = 0; j < numInstances; j++) {
				Object dataElement = dataList.get(j)[i];
				String type = processType(dataElement.toString());
				if(type.equals("STRING")) {
					nominalValues[i].add(dataElement.toString());
					numNominal++;
				} else {
					numericValues[i][j] = (double) dataElement;
					numNumeric++;
				}
			}
			
			if(numNominal > numNumeric) {
				isCategorical[i] = true;
			} else {
				isCategorical[i] = false;
			}
		}
		
		String[] binForInstance = null;
		FastVector attributeList = new FastVector();
		for(i = 0; i < numAttr; i++ ) {
			//special case for predictor since it must be nominal
			if(i == attributeIndex && !isCategorical[i]) {
				//create bins for numeric value
				Arrays.sort(numericValues[i]);
				BarChart chart = new BarChart(ArrayUtilityMethods.convertObjArrToDoubleArr(numericValues[i]));
				Hashtable<String, Object>[] bins = chart.getRetHashForJSON();
				binForInstance = chart.getAssignmentForEachObject();
				int numBins = bins.length;
				int z;
				FastVector nominalValuesInBin = new FastVector();
				for(z = 0; z < numBins; z++) {
					String binRange = (String) bins[z].get("x");
					nominalValuesInBin.addElement(binRange);
				}
				attributeList.addElement(new Attribute(names[i], nominalValuesInBin));
			} else if(isCategorical[i]) {
				FastVector nominalValuesInFV = new FastVector();
				HashSet<String> allPossibleValues = nominalValues[i];
				for(String val : allPossibleValues) {
					nominalValuesInFV.addElement(val);
				}
				attributeList.addElement(new Attribute(names[i], nominalValuesInFV));
			} else {
				attributeList.addElement(new Attribute(names[i]));
			}
		}
		//create the Instances Object to contain all the instance information
		Instances data = new Instances(nameDataSet, attributeList, numInstances);
		
		for(i = 0; i < numInstances; i++) {
			Instance dataEntry = new Instance(numAttr);
			dataEntry.setDataset(data);
			Object[] dataRow = dataList.get(i);
			for(j = 0; j < numAttr; j++) {
				if(j == attributeIndex && !isCategorical[j]) {
					dataEntry.setValue(j, binForInstance[i]);
				} else {
					Object valAttr = dataRow[j];
					if(valAttr.toString().isEmpty()) {
						dataEntry.setValue(j, "?"); // weka takes in "?" for missing data
					} else {
						if(isCategorical[j]) {
							dataEntry.setValue(j, valAttr.toString());
						} else {
							dataEntry.setValue(j, (double) valAttr);
						}
					}
				}
			}
//			System.out.println(dataEntry);
			data.add(dataEntry);
		}
		
		return data;
	}
	
	//TODO: THIS METHOD IS USED IN MULTIPLE PLACES
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
