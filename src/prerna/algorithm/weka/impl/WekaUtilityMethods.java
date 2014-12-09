package prerna.algorithm.weka.impl;

import java.util.ArrayList;
import java.util.HashSet;

import prerna.math.BarChart;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public final class WekaUtilityMethods {

	private WekaUtilityMethods() {

	}

	// cannot mix strings with doubles for attributes
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
				// ignore missing values to determine data type of column
				if(!dataElement.toString().isEmpty()) {
					String type = Utility.processType(dataElement.toString());
					if(type.equals("STRING")) {
						nominalValues[i].add(dataElement.toString());
						numNominal++;
					} else {
						numericValues[i][j] = (double) dataElement;
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

		String[] binForInstance = null;
		FastVector attributeList = new FastVector();
		for(i = 0; i < numAttr; i++ ) {
			//special case for predictor since it must be nominal
			if(i == attributeIndex && !isCategorical[i]) {
				//create bins for numeric value
				BarChart chart = new BarChart(ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(numericValues[i]));
				String[] binRange = chart.getNumericalBinOrder();
				binForInstance = chart.getAssignmentForEachObject();
				int numBins = binRange.length;
				int z;
				FastVector nominalValuesInBin = new FastVector();
				for(z = 0; z < numBins; z++) {
					nominalValuesInBin.addElement(binRange[z]);
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
					if(!valAttr.toString().isEmpty()) {
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
	
	public static double calculateAccuracy(FastVector predCorrect) {
		double sumPerCorrect = 0;
		int size = predCorrect.size();
		for (int i = 0; i < size; i++) {
			sumPerCorrect+= (double) predCorrect.elementAt(i);
		}

		return sumPerCorrect / size;
	}

	public static double calculatePercision(FastVector kappaValues) {
		double sumKappa = 0;
		int size = kappaValues.size();
		for (int i = 0; i < size; i++) {
			sumKappa+= (double) kappaValues.elementAt(i);
		}

		return sumKappa / size;
	}
}
