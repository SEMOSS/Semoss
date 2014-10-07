package prerna.algorithm.weka.impl;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.FastVector;
import weka.core.Instances;

public class WekaClassification {

	private Instances data;
	private double[] accuracy;
	private double[] precision;
	
	private String[] names;
	private ArrayList<Object[]> list;
	private Classifier model;
	
	PrintWriter writer = null;

	public WekaClassification(String name, ArrayList<Object[]> list, String[] names, String modelName) {
		this.list = list;
		this.names = names;
		this.model = ClassificationFactory.create(modelName);
		
		//writer only used for debugging
		//TODO: delete once stable
		if(writer == null) {
			try {
				writer = new PrintWriter("Classifier_Results_" + modelName + ".txt");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} 
	}

	public double[] getAccuracy() {
		return accuracy.clone();
	}

	public double[] getPrecision() {
		return precision.clone();
	}
	
	public void execute() throws Exception{
		int numAttributes = names.length;
		
		// Run for each attribute
		accuracy = new double[numAttributes - 1];
		precision = new double[numAttributes - 1];
		
		int i;
		for(i = 1; i < numAttributes; i++) {
			this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names, i);
			data.setClassIndex(i);
			// Do 10-split cross validation
			Instances[][] split = crossValidationSplit(data, 10);
	
			// Separate split into training and testing arrays
			Instances[] trainingSplits = split[0];
			Instances[] testingSplits = split[1];
	
			//Collect every group of predictions for current model in FastVector
			FastVector predCorrect = new FastVector();
			FastVector kappaValues = new FastVector();
			
			// For each training-testing split pair, train and test the classifier
			writer.println(names[i]);
			int j;
			for(j = 0; j < trainingSplits.length; j++) {
				Evaluation validation = classify(model, trainingSplits[j], testingSplits[j]);
				predCorrect.addElement(validation.pctCorrect());
				kappaValues.addElement(validation.kappa());
	
				writer.println(validation.toSummaryString(true));
				writer.println("Kappa Value                             " + validation.kappa());
				writer.println(validation.toMatrixString());
			}
	
			accuracy[i-1] = calculateAccuracy(predCorrect);
			precision[i-1] = calculatePercision(kappaValues);
			// Print current classifier's name and accuracy in a complicated,
			// but nice-looking way.
			writer.println("Accuracy for " + names[i] + ": "
					+ String.format("%.2f%%", accuracy[i-1])
					+ "\n---------------------------------");
			writer.println("Percision for " + names[i] + ": "
					+ String.format("%.2f%%", precision[i-1])
					+ "\n---------------------------------");
	
			System.out.println("Accuracy for " + names[i] + ": "
					+ String.format("%.2f%%", accuracy[i-1])
					+ "\n---------------------------------");
			System.out.println("Percision for " + names[i] + ": "
					+ String.format("%.2f", precision[i-1])
					+ "\n---------------------------------");
		}

		writer.close();
	}

	public Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
		Instances[][] split = new Instances[2][numberOfFolds];

		for (int i = 0; i < numberOfFolds; i++) {
			split[0][i] = data.trainCV(numberOfFolds, i);
			split[1][i] = data.testCV(numberOfFolds, i);
		}

		return split;
	}

	public Evaluation classify(Classifier model, Instances trainingSet, Instances testingSet) throws Exception {
		Evaluation evaluation = new Evaluation(trainingSet);

		model.buildClassifier(trainingSet);
		evaluation.evaluateModel(model, testingSet);

		return evaluation;
	}

	public double calculateAccuracy(FastVector predCorrect) {
		double sumPerCorrect = 0;
		int size = predCorrect.size();
		for (int i = 0; i < size; i++) {
			sumPerCorrect+= (double) predCorrect.elementAt(i);
		}

		return sumPerCorrect / size;
	}

	private double calculatePercision(FastVector kappaValues) {
		double sumKappa = 0;
		int size = kappaValues.size();
		for (int i = 0; i < size; i++) {
			sumKappa+= (double) kappaValues.elementAt(i);
		}

		return sumKappa / size;
	}
}
