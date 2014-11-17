package prerna.algorithm.weka.impl;

import weka.classifiers.Classifier;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.ADTree;
import weka.classifiers.trees.BFTree;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.J48graft;
import weka.classifiers.trees.LADTree;
import weka.classifiers.trees.LMT;
import weka.classifiers.trees.REPTree;
import weka.classifiers.trees.SimpleCart;

public final class ClassificationFactory {

	private ClassificationFactory() {
		
	}
	
	public static Classifier createClassifier(String type) {
		switch(type.toUpperCase()) {
		//tree outputs
		case "J48" : return new J48();
		case "J48GRAFT" : return new J48graft();
		case "SIMPLECART" : return new SimpleCart();
		case "REPTREE" : return new REPTree();
		case "BFTREE" : return new BFTree();
		// probability based
		case "ADTREE" : return new ADTree();
		case "LADTREE" : return new LADTree();
		//rule outputs
		case "PART" : return new PART();
		case "DECISIONTABLE" : return new DecisionTable();
		case "DECISIONSTUMP" : return new DecisionStump();
		case "LMT" : return new LMT();
		case "SIMPLELOGISTIC" : return new SimpleLogistic();
		}

		return null;
	}
	
}
