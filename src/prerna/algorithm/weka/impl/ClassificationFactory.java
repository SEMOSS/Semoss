package prerna.algorithm.weka.impl;

import weka.classifiers.Classifier;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.LMT;
import weka.classifiers.trees.REPTree;

public final class ClassificationFactory {

	private ClassificationFactory() {
		
	}
	
	public static Classifier create(String type) {
		switch(type) {
		case "J48" : return new J48();
		case "PART" : return new PART();
		case "DecisionTable" : return new DecisionTable();
		case "DecisionStump" : return new DecisionStump();
		case "REPTree" : return new REPTree();
		case "LMT" : return new LMT();
		case "SimpleLogistic" : return new SimpleLogistic();
		}

		return null;
	}

}
