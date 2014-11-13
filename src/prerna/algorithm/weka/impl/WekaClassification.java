package prerna.algorithm.weka.impl;

import java.util.ArrayList;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.DecisionStump;
import weka.core.Instances;

public class WekaClassification {

	private Instances data;
	
	private String[] names;
	private ArrayList<Object[]> list;
	private Classifier model;
	private String tree;
	
	public WekaClassification(String name, ArrayList<Object[]> list, String[] names, String modelName) {
		this.list = list;
		this.names = names;
		this.model = ClassificationFactory.create(modelName);
	}

	public String getTreeAsString() {
		return tree;
	}
	
	public void execute() throws Exception{
		int numAttributes = names.length;
		
		this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names, numAttributes - 1);
		data.setClassIndex(numAttributes - 1);
		// Do 10-split cross validation
		Instances[][] split = crossValidationSplit(data, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		double pctCorrect = -1; // assign any negative value
		// For each training-testing split pair, train and test the classifier
		int j;
		for(j = 0; j < trainingSplits.length; j++) {
			Evaluation validation = classify(model, trainingSplits[j], testingSplits[j]);
			double newPctCorrect = validation.pctCorrect();
			if(newPctCorrect > pctCorrect) {
				tree = model.toString();
			}
		}

		System.out.println(tree);
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
}
