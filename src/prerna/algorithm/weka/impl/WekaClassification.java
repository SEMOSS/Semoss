package prerna.algorithm.weka.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.FastVector;
import weka.core.Instances;

public class WekaClassification {

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
	
	public WekaClassification(ArrayList<Object[]> list, String[] names, String modelName, int classIndex) {
		this.list = list;
		this.names = names;
		this.model = ClassificationFactory.createClassifier(modelName);
		this.modelName = model.getClass().toString();
		this.classIndex = classIndex;
	}

	public Map<String, Map> getTreeMap() {
		return treeMap;
	}
	
	public void execute() throws Exception{
		this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names, classIndex);
		data.setClassIndex(classIndex);
		// Do 10-split cross validation
		Instances[][] split = WekaUtilityMethods.crossValidationSplit(data, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		accuracy = -1;
		precision = 0;
		// For each training-testing split pair, train and test the classifier
		int j;
		for(j = 0; j < trainingSplits.length; j++) {
			Evaluation validation = WekaUtilityMethods.classify(model, trainingSplits[j], testingSplits[j]);
			double newPctCorrect = validation.pctCorrect();
			if(newPctCorrect > accuracy) {
				treeAsString = model.toString();
				accuracy = newPctCorrect;
				precision = validation.kappa();
			}
			
			// keep track of accuracy and percision of each test
			accuracyArr.add(newPctCorrect);
			precisionArr.add(validation.kappa());
		}
		
		System.out.println("Accuracy: " + accuracy);
		System.out.println("Precision: " + precision);
	}
	
	public void processTreeString() {
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
