package prerna.algorithm.weka.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;

public class WekaClassification {

	private Instances data;
	
	private String[] names;
	private ArrayList<Object[]> list;
	private Classifier model;
	private String treeAsString;
	
	private String[] j48TreeStringArr = null;
	private Map<String, Map> j48Tree = new HashMap<String, Map>();
	
	int index; 
	
	public WekaClassification(ArrayList<Object[]> list, String[] names, String modelName) {
		this.list = list;
		this.names = names;
		this.model = ClassificationFactory.createClassifier(modelName);
	}

	public String getTreeAsString() {
		return treeAsString;
	}
	
	public Map<String, Map> getJ48Tree() {
		return j48Tree;
	}
	
	public void execute() throws Exception{
		int numAttributes = names.length;
		
		this.data = WekaUtilityMethods.createInstancesFromQuery("Test", list, names, numAttributes - 1);
		data.setClassIndex(numAttributes - 1);
		// Do 10-split cross validation
		Instances[][] split = WekaUtilityMethods.crossValidationSplit(data, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		double pctCorrect = -1; // assign any negative value
		// For each training-testing split pair, train and test the classifier
		int j;
		for(j = 0; j < trainingSplits.length; j++) {
			Evaluation validation = WekaUtilityMethods.classify(model, trainingSplits[j], testingSplits[j]);
			double newPctCorrect = validation.pctCorrect();
			if(newPctCorrect > pctCorrect) {
				treeAsString = model.toString();
			}
		}

		processTreeString(model.getClass().toString());
	}
	
	private void processTreeString(String type) {
		String[] treeSplit = treeAsString.split("\n");
		j48Tree = new HashMap<String, Map>();
		if(type.contains("J48")) {
			j48TreeStringArr = new String[treeSplit.length - 7];
			// indices based on weka J48 decision tree output
			System.arraycopy(treeSplit, 3, j48TreeStringArr, 0, j48TreeStringArr.length);
			// to debugg
//			for(int i = 0; i < j48TreeStringArr.length; i++) {
//				System.out.println(i + ": " + j48TreeStringArr[i]);
//			}
			generateJ48Tree(j48Tree, "", 0);
		}
		
	}
	
	private void generateJ48Tree(Map<String, Map> rootMap, String startKey, int subTreeIndex) {
		String endRegex = "(.*\\(\\d+\\.\\d/\\d+\\.\\d\\))|(.*\\(\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(.*\\(\\d+\\.\\d/\\d+\\.\\d\\))|(.*\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";
		String lastRegex = "(\\(\\d+\\.\\d/\\d+\\.\\d\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d/\\d+\\.\\d\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";
				
		Map<String, Map> currTree = new HashMap<String, Map>();
		if(!startKey.isEmpty()) {
			rootMap.put(startKey, currTree);
		}
		
		for(; index < j48TreeStringArr.length; index++) {
			System.out.println(index);
			String row = j48TreeStringArr[index];
			if(!row.startsWith("|")) {
				if(row.matches(endRegex)) {
					String[] keyVal = row.replaceFirst(lastRegex, "").split(": ");
					Map<String, Map> endMap = new HashMap<String, Map>();
					endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
					rootMap.put(keyVal[0].trim(), endMap);
				} else {
					String newRow = row.trim();
					rootMap.put(newRow, currTree);
					startKey = newRow;
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
				int newSubTreeIndex = j48TreeStringArr[index].lastIndexOf("| ");
				generateJ48Tree(currTree, newKey, newSubTreeIndex);
			}
		}
	}
	
	
	
	
	
}
