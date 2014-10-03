package prerna.algorithm.weka.impl;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.LMT;
import weka.classifiers.trees.REPTree;
import weka.core.FastVector;
import weka.core.Instances;

public class WekaClassification {

	private Instances data;
	private double[] accuracy;
	private double[] percision;
	private String[] classNames;

	PrintWriter writer = null;

	public WekaClassification(String name, ArrayList<Object[]> list, String[] names) {
		data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names);
		
		if(writer == null) {
			try {
				writer = new PrintWriter("Classifier_Results" + names[names.length - 1] + ".txt");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} 
	}

	public double[] getAccuracy() {
		return accuracy.clone();
	}

	public double[] getPercision() {
		return percision.clone();
	}
	
	public String[] getClassNames() {
		return classNames.clone();
	}

	public void execute() throws Exception{
		data.setClassIndex(data.numAttributes() - 1);

		// Do 10-split cross validation
		Instances[][] split = crossValidationSplit(data, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];

		// Use a set of classifiers
		Classifier[] models = { 
				new J48(), // a decision tree
				new PART(), 
				new DecisionTable(),//decision table majority classifier
				new DecisionStump(), //one-level decision tree
				new REPTree(),
//				new LMT(),
				new SimpleLogistic()
		};

		// Run for each model
		accuracy = new double[6];
		percision = new double[6];
		classNames = new String[6];
		for (int j = 0; j < models.length; j++) {
			//			for(int z = 1; z <= 10; z++) {
			//				float confVal = z * 0.1f;
			//				
			//				if(j == 0){
			//					((J48) models[j]).setConfidenceFactor(confVal);
			//				} else if(j == 1) {
			//					((PART) models[j]).setConfidenceFactor(confVal);
			//				}

			//Collect every group of predictions for current model in FastVector
			FastVector predCorrect = new FastVector();
			FastVector kappaValues = new FastVector();
			// For each training-testing split pair, train and test the classifier
			writer.println(models[j].getClass().getSimpleName());
			for (int i = 0; i < trainingSplits.length; i++) {
				Evaluation validation = classify(models[j], trainingSplits[i], testingSplits[i]);
				predCorrect.addElement(validation.pctCorrect());
				kappaValues.addElement(validation.kappa());

				writer.println(validation.toSummaryString(true));
				writer.println("Kappa Value                             " + validation.kappa());
				writer.println(validation.toMatrixString());
			}

			accuracy[j] = calculateAccuracy(predCorrect);
			percision[j] = calculatePercision(kappaValues);
			classNames[j] = models[j].getClass().getSimpleName();
			// Print current classifier's name and accuracy in a complicated,
			// but nice-looking way.
			writer.println("Accuracy of " + classNames[j] + ": "
					+ String.format("%.2f%%", accuracy[j])
					+ "\n---------------------------------");
			writer.println("Percision of " + classNames[j] + ": "
					+ String.format("%.2f%%", percision[j])
					+ "\n---------------------------------");

			System.out.println("Accuracy of " + classNames[j] + ": "
					+ String.format("%.2f%%", accuracy[j])
					+ "\n---------------------------------");
			System.out.println("Percision of " + classNames[j] + ": "
					+ String.format("%.2f", percision[j])
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
