package prerna.algorithm.weka.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.main.AbstractFileReader;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.BFTree;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.J48graft;
import weka.classifiers.trees.REPTree;
import weka.classifiers.trees.SimpleCart;
import weka.core.FastVector;
import weka.core.Instances;

public class WekaClassification {

	private static final Logger LOGGER = LogManager.getLogger(AbstractFileReader.class.getName());

	private Instances data;
	
	private String[] names;
	private ArrayList<Object[]> list;
	private Classifier model;
	private String treeAsString;
	
	private double accuracy;
	private double precision;
	
	private String[] treeStringArr = null;
	private Map<String, Map> treeMap = new HashMap<String, Map>();
	int index; 
	
	private String modelName;
	private int classIndex;
	
	private FastVector<Double> accuracyArr = new FastVector<Double>();
	private FastVector<Double> precisionArr = new FastVector<Double>();
	
	public FastVector<Double> getAccuracyArr() {
		return accuracyArr;
	}

	public FastVector<Double> getPrecisionArr() {
		return precisionArr;
	}
	
	public double getAccuracy() {
		return accuracy;
	}
	
	public double getPrecision() {
		return precision;
	}
	
	public String getTreeAsString() {
		return treeAsString;
	}
	
	/**
	 * Constructor to run classification algorithms in WEKA package
	 * @param list			ArrayList<Object[]> containing the data for each instance
	 * @param names			String[] containing the list of the variable names corresponding to each column in list
	 * @param modelName		String containing the name of the algorithm to run.  The list of valid inputs is listed below.
	 * @param classIndex	Integer corresponding to the column number that is being classified 
	 * 
	 * List of appropriate algorithms to run are: 
	 * 		These algorithms require you to classify nominal values
	 * 		1) J48 - Implementation of popular C4.5 algorithm.  For more information visit: http://en.wikipedia.org/wiki/C4.5_algorithm
	 * 		2) J48Graft - Grafting on C4.5 algorithm. For more information visit: http://ijcai.org/Past%20Proceedings/IJCAI-99%20VOL-2/PDF/007.pdf
	 * 		3) SimpleCart - Minimal Cost-Complexity Pruning algorithm. For more information visit: https://onlinecourses.science.psu.edu/stat557/node/93
	 * 		4) BFTree - Best First Tree algorithm. For more information visit: http://researchcommons.waikato.ac.nz/bitstream/handle/10289/2317/thesis.pdf?sequence=1&isAllowed=y
	 * 		These algorithms can be used to classify real number values
	 * 	 	5) REPTree - Regression Tree algorithm. For more information visit:    
	 */
	public WekaClassification(ArrayList<Object[]> list, String[] names, String modelName, int classIndex) {
		this.list = list;
		this.names = names;
		this.model = ClassificationFactory.createClassifier(modelName);
		this.modelName = model.getClass().toString();
		this.classIndex = classIndex;
		
		LOGGER.info("Starting classification algorithm using " + modelName + " to predict variable " + names[classIndex] + "...");
	}

	public Map<String, Map> getTreeMap() {
		return treeMap;
	}
	
	//error will be thrown when trying to classify a variable that is always the same value
	public void execute() throws Exception{
		LOGGER.info("Generating Weka Instances object...");
//		if(modelName.contains("REPTree")) {
//			this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names);
//		} else {
			this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names, classIndex);
//		}
		// setting class attribute
		data.setClassIndex(classIndex);
		
		LOGGER.info("Performing 10-fold cross-validation split of data...");
		Instances[][] split = WekaUtilityMethods.crossValidationSplit(data, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		// For each training-testing split pair, train and test the classifier
		int j;
		double bestTreeAccuracy = -1;
		for(j = 0; j < trainingSplits.length; j++) {
			LOGGER.info("Running classification on training and test set number " + j + "...");
			Evaluation validation = WekaUtilityMethods.classify(model, trainingSplits[j], testingSplits[j]);
			double newPctCorrect = validation.pctCorrect();
			if(newPctCorrect > bestTreeAccuracy) {
				treeAsString = model.toString();
				bestTreeAccuracy = newPctCorrect;
			}
			
			// keep track of accuracy and precision of each test
			accuracyArr.add(newPctCorrect);
//			try {
				double precisionVal = validation.precision(1);
				precisionArr.add(precisionVal*100);
//			} catch(NullPointerException ex) {
//				// do nothing about algorithm not providing a method to get precision
//			}
		}
		
		accuracy = WekaUtilityMethods.calculateAccuracy(accuracyArr);
		precision = WekaUtilityMethods.calculateAccuracy(precisionArr);
	}
	
	public void processTreeString() {
		LOGGER.info("Generating Tree Map from classification tree string...");
		String[] treeSplit = treeAsString.split("\n");
		treeMap = new HashMap<String, Map>();
		// exception case when tree is a single node
		if(modelName.contains("J48")) {
			if(treeSplit.length == 7 && treeSplit[6].equals("Size of the tree : 	1")) {
				generateNodeTreeWithParenthesis(treeMap, treeSplit[2]);
			} else {
				treeStringArr = new String[treeSplit.length - 7];
				// indices based on weka J48 decision tree output
				System.arraycopy(treeSplit, 3, treeStringArr, 0, treeStringArr.length);
				generateTreeEndingWithParenthesis(treeMap, "", 0);
			}
		} else if(modelName.contains("SimpleCart")) {
			if(treeSplit.length == 6 && treeSplit[5].equals("Size of the Tree: 1")) {
				generateNodeTreeWithParenthesis(treeMap, treeSplit[1]);
			} else {
				treeStringArr = new String[treeSplit.length - 6];
				// indices based on weka J48 decision tree output
				System.arraycopy(treeSplit, 2, treeStringArr, 0, treeStringArr.length);
				generateTreeEndingWithParenthesis(treeMap, "", 0);
			}
		} else if(modelName.contains("REPTree")) {
			if(treeSplit.length == 6 && treeSplit[5].equals("Size of the tree : 1")) {
				generateNodeTreeWithParenthesisAndBrackets(treeMap, treeSplit[3]);
			} else {
				treeStringArr = new String[treeSplit.length - 6];
				// indices based on weka J48 decision tree output
				System.arraycopy(treeSplit, 4, treeStringArr, 0, treeStringArr.length);
				generateTreeEndingWithParenthesisAndBrackets(treeMap, "", 0);
			}
		} else if(modelName.contains("BFTree")) {
			if(treeSplit.length == 6 && treeSplit[5].equals("Number of Leaf Nodes: 1")) {
				generateNodeTreeWithParenthesis(treeMap, treeSplit[1]);
			} else {
				treeStringArr = new String[treeSplit.length - 6];
				// indices based on weka J48 decision tree output
				System.arraycopy(treeSplit, 2, treeStringArr, 0, treeStringArr.length);
				generateTreeEndingWithParenthesis(treeMap, "", 0);
			}
		}
	}
	
	private void generateNodeTreeWithParenthesis(Map<String, Map> rootMap, String nodeValue) {
		String lastRegex = "(\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";

		String key = nodeValue.replaceFirst(":", "").replaceFirst(lastRegex, "").trim();
		rootMap.put(key, new HashMap<String, Map>());
	}
	
	private void generateNodeTreeWithParenthesisAndBrackets(Map<String, Map> rootMap, String nodeValue) {
		String lastRegex = "((\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+/\\d+\\))|(\\(\\d+\\|\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\))|(\\(\\d+/\\d+\\.\\d+\\)))\\s((\\[\\d+\\.\\d+/\\d+\\.\\d+\\])|(\\[\\d+/\\d+\\])|(\\[\\d+\\.\\d+/\\d+\\])|(\\[\\d+/\\d+\\.\\d+\\]))";

		String key = nodeValue.replaceFirst(":", "").replaceFirst(lastRegex, "").trim();
		rootMap.put(key, new HashMap<String, Map>());
	}

	private void generateTreeEndingWithParenthesis(Map<String, Map> rootMap, String startKey, int subTreeIndex) {
		String endRegex = "(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";
		String lastRegex = "(\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";
				
		Map<String, Map> currTree = new HashMap<String, Map>();
		if(!startKey.isEmpty()) {
			rootMap.put(startKey, currTree);
		}
		
		for(; index < treeStringArr.length; index++) {
			String row = treeStringArr[index];
			if(!row.startsWith("|")) {
				if(subTreeIndex > 0) {
					index--;
					return;
				} 
				if(row.matches(endRegex)) {
					String[] keyVal = row.replaceFirst(lastRegex, "").split(": ");
					Map<String, Map> endMap = new HashMap<String, Map>();
					endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
					rootMap.put(keyVal[0].trim(), endMap);
				} else {
					String newRow = row.trim();
					rootMap.put(newRow, currTree);
					startKey = newRow;
					subTreeIndex = 0;
				}
			} else if(row.lastIndexOf("| ") != subTreeIndex) {
				index--;
				return;
			} else if(row.matches(endRegex)) {
				String[] keyVal = row.substring(row.lastIndexOf("| ")+1, row.length()).trim().replaceFirst(lastRegex, "").split(": ");
				Map<String, Map> endMap = new HashMap<String, Map>();
				endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
				currTree.put(keyVal[0].trim(), endMap);
			} else {
				index++;
				String newKey = row.substring(row.lastIndexOf("| ")+1, row.length()).trim();
				// for a subtree to exist, there must be a new row after
				int newSubTreeIndex = treeStringArr[index].lastIndexOf("| ");
				generateTreeEndingWithParenthesis(currTree, newKey, newSubTreeIndex);
			}
		}
	}
	
	private void generateTreeEndingWithParenthesisAndBrackets(Map<String, Map> rootMap, String startKey, int subTreeIndex) {
		String endRegex = "((.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(.*\\(\\d+/\\d+\\))|(.*\\(\\d+\\|\\d+\\))|(.*\\(\\d+\\.\\d+/\\d+\\))|(\\(\\d+/\\d+\\.\\d+\\)))\\s((\\[\\d+\\.\\d+/\\d+\\.\\d+\\])|(\\[\\d+/\\d+\\])|(\\[\\d+\\.\\d+/\\d+\\])|(\\[\\d+/\\d+\\.\\d+\\]))";
		String lastRegex = "((\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+/\\d+\\))|(\\(\\d+\\|\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\))|(\\(\\d+/\\d+\\.\\d+\\)))\\s((\\[\\d+\\.\\d+/\\d+\\.\\d+\\])|(\\[\\d+/\\d+\\])|(\\[\\d+\\.\\d+/\\d+\\])|(\\[\\d+/\\d+\\.\\d+\\]))";
		Map<String, Map> currTree = new HashMap<String, Map>();
		if(!startKey.isEmpty()) {
			rootMap.put(startKey, currTree);
		}
		
		for(; index < treeStringArr.length; index++) {
			String row = treeStringArr[index];
			if(!row.startsWith("|")) {
				if(subTreeIndex > 0) {
					index--;
					return;
				} 
				if(row.matches(endRegex)) {
					String[] keyVal = row.replaceFirst(lastRegex, "").split(": ");
					Map<String, Map> endMap = new HashMap<String, Map>();
					endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
					rootMap.put(keyVal[0].trim(), endMap);
				} else {
					String newRow = row.trim();
					rootMap.put(newRow, currTree);
					startKey = newRow;
					subTreeIndex = 0;
				}
			} else if(row.lastIndexOf("| ") != subTreeIndex) {
				index--;
				return;
			} else if(row.matches(endRegex)) {
				String[] keyVal = row.substring(row.lastIndexOf("| ")+1, row.length()).trim().replaceFirst(lastRegex, "").split(": ");
				Map<String, Map> endMap = new HashMap<String, Map>();
				endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
				currTree.put(keyVal[0].trim(), endMap);
			} else {
				index++;
				String newKey = row.substring(row.lastIndexOf("| ")+1, row.length()).trim();
				// for a subtree to exist, there must be a new row after
				int newSubTreeIndex = treeStringArr[index].lastIndexOf("| ");
				generateTreeEndingWithParenthesisAndBrackets(currTree, newKey, newSubTreeIndex);
			}
		}
	}
	
}
