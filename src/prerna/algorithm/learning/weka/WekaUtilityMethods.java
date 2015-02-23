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
package prerna.algorithm.learning.weka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import prerna.math.BarChart;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public final class WekaUtilityMethods {

	private WekaUtilityMethods() {

	}

	// cannot mix strings with doubles for attributes
	public static Instances createInstancesFromQueryUsingBinNumerical(String nameDataSet, ArrayList<Object[]> dataList, String[] names) {
		int numInstances = dataList.size();	
		int i;
		int numAttr = names.length;
		boolean[] isCategorical = new boolean[numAttr];
		HashSet<String>[] nominalValues = new HashSet[numAttr];
		Double[][] numericValues = new Double[numAttr][numInstances];
		preProcessQueryData(dataList, nominalValues, numericValues, isCategorical, numInstances, numAttr);

		int numNumeric = 0;
		for(i = 0; i < numAttr; i++) {
			if(!isCategorical[i]) {
				numNumeric++;
			}
		}
		
		String[][] binForInstance = new String[numNumeric][numInstances];
		int counter = 0;
		ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
		for(i = 0; i < numAttr; i++ ) {
			//special case for predictor since it must be nominal
			if(isCategorical[i]) {
				ArrayList<String> nominalValuesInFV = new ArrayList<String>();
				HashSet<String> allPossibleValues = nominalValues[i];
				for(String val : allPossibleValues) {
					nominalValuesInFV.add(val);
				}
				attributeList.add(new Attribute(names[i], nominalValuesInFV));
			} else {
				//create bins for numeric value
				BarChart chart = new BarChart(ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(numericValues[i]));
				String[] binRange = chart.getNumericalBinOrder();
				binForInstance[counter] = chart.getAssignmentForEachObject();
				counter++;
				int numBins = binRange.length;
				int z;
				ArrayList<String> nominalValuesInBin = new ArrayList<String>();
				for(z = 0; z < numBins; z++) {
					nominalValuesInBin.add(binRange[z]);
				}
				attributeList.add(new Attribute(names[i], nominalValuesInBin));
			}
		}
		//create the Instances Object to contain all the instance information
		Instances data = new Instances(nameDataSet, attributeList, numInstances);
		fillInstances(data, dataList, numericValues, binForInstance, isCategorical, numInstances, numAttr);

		return data;
	}
	
	// cannot mix strings with doubles for attributes
	public static Instances createInstancesFromQuery(String nameDataSet, ArrayList<Object[]> dataList, String[] names, int attributeIndex) {
		int numInstances = dataList.size();	
		int i;
		int numAttr = names.length;
		boolean[] isCategorical = new boolean[numAttr];
		HashSet<String>[] nominalValues = new HashSet[numAttr];
		Double[][] numericValues = new Double[numAttr][numInstances];
		preProcessQueryData(dataList, nominalValues, numericValues, isCategorical, numInstances, numAttr);

		String[] binForInstance = null;
		ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
		for(i = 0; i < numAttr; i++ ) {
			//special case for predictor since it must be nominal
			if(i == attributeIndex && !isCategorical[i]) {
				//create bins for numeric value
				BarChart chart = new BarChart(ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(numericValues[i]));
				String[] binRange = chart.getNumericalBinOrder();
				binForInstance = chart.getAssignmentForEachObject();
				int numBins = binRange.length;
				int z;
				ArrayList<String> nominalValuesInBin = new ArrayList<String>();
				for(z = 0; z < numBins; z++) {
					nominalValuesInBin.add(binRange[z]);
				}
				attributeList.add(new Attribute(names[i], nominalValuesInBin));
			} else if(isCategorical[i]) {
				ArrayList<String> nominalValuesInFV = new ArrayList<String>();
				HashSet<String> allPossibleValues = nominalValues[i];
				for(String val : allPossibleValues) {
					nominalValuesInFV.add(val);
				}
				attributeList.add(new Attribute(names[i], nominalValuesInFV));
			} else {
				attributeList.add(new Attribute(names[i]));
			}
		}
		//create the Instances Object to contain all the instance information
		Instances data = new Instances(nameDataSet, attributeList, numInstances);
		fillInstances(data, dataList, numericValues, binForInstance, isCategorical, numInstances, numAttr, attributeIndex);

		return data;
	}
	
	// process information to determine data types
	private static void preProcessQueryData(ArrayList<Object[]> dataList, HashSet<String>[] nominalValues, Double[][] numericValues, boolean[] isCategorical, int numInstances, int numAttr) {
		int i;
		int j;
		
		for(i = 0; i < numAttr; i++) {
			if(nominalValues[i] == null) {
				nominalValues[i] = new HashSet<String>();
			}

			int numNominal = 0;
			int numNumeric = 0;
			for(j = 0; j < numInstances; j++) {
				Object dataElement = dataList.get(j)[i];
				// ignore missing values to determine data type of column
				if(!dataElement.toString().isEmpty() && !dataElement.toString().equals("?")) {
					String type = Utility.processType(dataElement.toString());
					if(type.equals("STRING")) {
						nominalValues[i].add(dataElement.toString());
						numNominal++;
					} else {
						try {
							numericValues[i][j] = (double) dataElement;
						} catch(ClassCastException ex) {
							numericValues[i][j] = Double.parseDouble(dataElement.toString());
						}
						numNumeric++;
					}
				}
			}

			if(numNominal > numNumeric) {
				isCategorical[i] = true;
			} else {
				isCategorical[i] = false;
			}
		}
	}
	
	// fills in Instances object
	private static void fillInstances(Instances data, ArrayList<Object[]> dataList, Double[][] numericValues, String[][] binForInstance, boolean[] isCategorical, int numInstances, int numAttr) {
		int i;
		int j;
		
		for(i = 0; i < numInstances; i++) {
			Instance dataEntry = new DenseInstance(numAttr);
			dataEntry.setDataset(data);
			Object[] dataRow = dataList.get(i);
			int counter = 0;
			for(j = 0; j < numAttr; j++) {
				if(!isCategorical[j]) {
					String val = binForInstance[counter][i];
					if(!val.equals("NaN")) {
						dataEntry.setValue(j, val);
					}
					counter++;
				} else {
					Object valAttr = dataRow[j];
					if(!valAttr.toString().isEmpty() && !valAttr.toString().equals("?")) {
						if(isCategorical[j]) {
							dataEntry.setValue(j, valAttr.toString());
						} else {
							dataEntry.setValue(j, numericValues[j][i]); // take the numeric values to prevent re-casting
						}
					}
				}
			}
//			System.out.println(dataEntry);
			data.add(dataEntry);
		}
	}
	
	// fills in Instances object
	private static void fillInstances(Instances data, ArrayList<Object[]> dataList, Double[][] numericValues, String[] binForInstance, boolean[] isCategorical, int numInstances, int numAttr, int attributeIndex) {
		int i;
		int j;
		
		for(i = 0; i < numInstances; i++) {
			Instance dataEntry = new DenseInstance(numAttr);
			dataEntry.setDataset(data);
			Object[] dataRow = dataList.get(i);
			for(j = 0; j < numAttr; j++) {
				if(j == attributeIndex && !isCategorical[j]) {
					String val = binForInstance[i];
					if(!val.equals("NaN")) {
						dataEntry.setValue(j, binForInstance[i]);
					}
				} else {
					Object valAttr = dataRow[j];
					if(!valAttr.toString().isEmpty() && !valAttr.toString().equals("?")) {
						if(isCategorical[j]) {
							dataEntry.setValue(j, valAttr.toString());
						} else {
							dataEntry.setValue(j, numericValues[j][i]); // take the numeric values to prevent re-casting
						}
					}
				}
			}
//			System.out.println(dataEntry);
			data.add(dataEntry);
		}
	}
	
	public static Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
		Instances[][] split = new Instances[2][numberOfFolds];

		for (int i = 0; i < numberOfFolds; i++) {
			split[0][i] = data.trainCV(numberOfFolds, i);
			split[1][i] = data.testCV(numberOfFolds, i);
		}

		return split;
	}

	public static Evaluation classify(Classifier model, Instances trainingSet, Instances testingSet) throws Exception {
		Evaluation evaluation = new Evaluation(trainingSet);

		model.buildClassifier(trainingSet);
		evaluation.evaluateModel(model, testingSet);

		return evaluation;
	}
	
	public static double calculateAccuracy(List<Double> predCorrect) {
		double sumPerCorrect = 0;
		int size = predCorrect.size();
		for (int i = 0; i < size; i++) {
			sumPerCorrect+= (double) predCorrect.get(i);
		}

		return sumPerCorrect / size;
	}

	public static double calculatePercision(List<Double> kappaValues) {
		double sumKappa = 0;
		int size = kappaValues.size();
		for (int i = 0; i < size; i++) {
			sumKappa+= (double) kappaValues.get(i);
		}

		return sumKappa / size;
	}
}
