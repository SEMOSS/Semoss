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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;

public class WekaClassification implements IAnalyticRoutine {

	private static final Logger LOGGER = LogManager.getLogger(WekaClassification.class.getName());

	private static final String MODEL_NAME = "modelName";
	private static final String CLASS_INDEX = "classIndex";
	private static final String SKIP_ATTRIBUTES = "skipAttributes";

	private Instances instancesData;
	private String[] names;
	
	private List<SEMOSSParam> options;

	private String modelName;
	private int classIndex;
	
	private Classifier model;
	private String treeAsString;
	
	private String[] treeStringArr = null;
	private Map<String, Map> treeMap = new HashMap<String, Map>();
	int index; 
	
	private double bestAccuracy; // currently using model with best accuracy
	private double avgAccuracy;
	private double avgPrecision;
	private List<Double> accuracyArr = new ArrayList<Double>();
	private List<Double> precisionArr = new ArrayList<Double>();

	private List<String> skipAttributes;
	
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
	public WekaClassification() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(MODEL_NAME);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(CLASS_INDEX);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(SKIP_ATTRIBUTES);
		options.add(2, p3);
	
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.modelName = (String) options.get(0).getSelected();
		this.classIndex = (int) options.get(1).getSelected();
		this.skipAttributes = (List<String>) options.get(2).getSelected();

		ITableDataFrame dataFrame = data[0];
		dataFrame.setColumnsToSkip(skipAttributes);
		this.names = dataFrame.getColumnHeaders();
		this.model = ClassificationFactory.createClassifier(modelName);

		LOGGER.info("Starting classification algorithm using " + modelName + " to predict variable " + names[classIndex] + "...");
		LOGGER.info("Generating Weka Instances object...");
		this.instancesData = WekaUtilityMethods.createInstancesFromQueryUsingBinNumerical("Apriori dataset", dataFrame.getData(), names);
		instancesData.setClassIndex(classIndex);
		
		// cannot classify when only one value
		if(instancesData.numDistinctValues(classIndex) == 1) {
			LOGGER.info("There is only one distinct value for column " + names[classIndex]);
			avgAccuracy = 100;
			avgPrecision = 100;
			return null;
		}
		
		LOGGER.info("Performing 10-fold cross-validation split of data...");
		Instances[][] split = WekaUtilityMethods.crossValidationSplit(instancesData, 10);

		// Separate split into training and testing arrays
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		// For each training-testing split pair, train and test the classifier
		int j;
		for(j = 0; j < trainingSplits.length; j++) {
			LOGGER.info("Running classification on training and test set number " + j + "...");
			Evaluation validation = null;
			try {
				validation = WekaUtilityMethods.classify(model, trainingSplits[j], testingSplits[j]);
			} catch (Exception e) {
				e.printStackTrace();
			}
			double newPctCorrect = validation.pctCorrect();
			// ignore when weka gives a NaN for accuracy -> occurs when every instance in training set is unknown for variable being classified
			if(Double.isNaN(newPctCorrect)) {
				LOGGER.info("Cannot use this classification since every instance in training set is unknown for " + names[classIndex]);
			} else {
				if(newPctCorrect > bestAccuracy) {
					treeAsString = model.toString();
					bestAccuracy = newPctCorrect;
				}
				
				// keep track of accuracy and precision of each test
				accuracyArr.add(newPctCorrect);
				double precisionVal = validation.precision(1);
				precisionArr.add(precisionVal*100);
			}
		}
		
		avgAccuracy = WekaUtilityMethods.calculateAverage(accuracyArr);
		avgPrecision = WekaUtilityMethods.calculateAverage(precisionArr);
		
		return null;
	}
	
	public void processTreeString() {
		LOGGER.info("Generating Tree Map from classification tree string...");
		if(treeAsString == null) {
			if(instancesData.numDistinctValues(classIndex) == 1) {
				treeMap = new HashMap<String, Map>();
				treeMap.put(instancesData.get(0).attribute(classIndex).value(classIndex), new HashMap());
				return;
			}
		}
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

	@Override
	public String getName() {
		return "Classification Routine";
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.WekaAprioriVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public List<Double> getAccuracyArr() {
		return accuracyArr;
	}

	public List<Double> getPrecisionArr() {
		return precisionArr;
	}
	
	public double getAccuracy() {
		return avgAccuracy;
	}
	
	public double getPrecision() {
		return avgPrecision;
	}
	
	public String getTreeAsString() {
		return treeAsString;
	}
	
	public Map<String, Map> getTreeMap() {
		return treeMap;
	}
}
