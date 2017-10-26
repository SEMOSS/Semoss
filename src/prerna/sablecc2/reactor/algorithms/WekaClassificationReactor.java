package prerna.sablecc2.reactor.algorithms;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;

public class WekaClassificationReactor extends AbstractReactor {

	private static final String CLASS_NAME = WekaClassificationReactor.class.getName();

	private static final String CLASSIFICATION_COLUMN = "classify";
	private static final String ATTRIBUTES = "attributes";
	private static final String PANEL = "panel";

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		dataFrame.setLogger(logger);
		
		// figure out inputs
		String predictionCol = getClassificationColumn(logger);
		List<String> attributes = getColumns();
		int numCols = attributes.size();
		if(numCols == 0) {
			String errorString = "No columns were passed as attributes for the classification routine.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		// in case the attributes col has a repeat
		if(attributes.contains(predictionCol)) {
			attributes.remove(predictionCol);
			numCols--;
		}
		
		// I need to return back headers
		// and a dataTableAlign object
		// in addition to the specific correlation data
		String[] retHeaders = new String[numCols+1];
		boolean[] isNumeric = new boolean[numCols+1];
		
		QueryStruct2 qs = new QueryStruct2();
		// add the predictor column
		QueryColumnSelector predictorHead = new QueryColumnSelector();
		if(predictionCol.contains("__")) {
			String[] split = predictionCol.split("__");
			predictorHead.setTable(split[0]);
			predictorHead.setColumn(split[1]);
			retHeaders[0] = split[1];
		} else {
			predictorHead.setTable(predictionCol);
			retHeaders[0] = predictionCol;
		}
		isNumeric[0] = dataFrame.isNumeric(predictionCol);
		qs.addSelector(predictorHead);
		// add the feature columns
		for(int i = 0; i < numCols; i++) {
			String header = attributes.get(i);
			QueryColumnSelector qsHead = new QueryColumnSelector();
			if(header.contains("__")) {
				String[] split = header.split("__");
				qsHead.setTable(split[0]);
				qsHead.setColumn(split[1]);
				retHeaders[i+1] = split[1];
			} else {
				qsHead.setTable(header);
				retHeaders[i+1] = header;
			}
			isNumeric[i+1] = dataFrame.isNumeric(header);
			qs.addSelector(qsHead);
		}
		qs.mergeFilters(dataFrame.getFrameFilters());

		int numRows = getNumRows(dataFrame, predictorHead);
		Iterator<IHeadersDataRow> it = dataFrame.query(qs);

		logger.info("Start converting frame into WEKA Instacnes data structure");
		Instances data = WekaReactorHelper.genInstances(retHeaders, isNumeric, numRows);
		data = WekaReactorHelper.fillInstances(data, it, isNumeric, logger);
		logger.info("Done converting frame into WEKA Instacnes data structure");
		if(isNumeric[0]) {
			logger.info("Can only run classification on categorical data, must discretize numerical column");
			// one based for some weird reason..
			data = WekaReactorHelper.discretizeNumericField(data, "1");
			logger.info("Done with discretizing numerical column");
		}
		data.setClassIndex(0);

		double accuracy = -1;
		double precision = -1;
		
		if(data.numDistinctValues(0) == 1) {
			logger.info("There is only one distinct value for column " + retHeaders[0]);
			accuracy = 100;
			precision = 100;
			//TODO: make the return object here and now and be done with it
		} else if(data.numDistinctValues(0) == data.size()) {
			String errorString = "The column to predict, " + retHeaders[0] + ", is a unique identifier in this table. Does not make sense to classify it.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		
		// Separate split into training and testing arrays
		Instances[][] split = crossValidationSplit(data, 10);
		Instances[] trainingSplits = split[0];
		Instances[] testingSplits = split[1];
		
		J48 model = new J48();
		String treeAsString = "";
		// For each training-testing split pair, train and test the classifier
		logger.info("Performing 10-fold cross validation to determine best model");
		int j;
		for(j = 0; j < trainingSplits.length; j++) {
			logger.info("Running classification on training and test set number " + (j+1) + "...");
			Evaluation validation = null;
			try {
				validation = classify(model, trainingSplits[j], testingSplits[j]);
			} catch (Exception e) {
				e.printStackTrace();
			}
			double newPctCorrect = validation.pctCorrect();
			// ignore when weka gives a NaN for accuracy -> occurs when every instance in training set is unknown for variable being classified
			if(Double.isNaN(newPctCorrect)) {
				logger.info("Cannot use this classification since every instance in training set is unknown for " + retHeaders[0]);
			} else {
				if(newPctCorrect > accuracy) {
					treeAsString = model.toString();
					accuracy = newPctCorrect;
					precision = validation.precision(1);
				}
			}
		}
		logger.info("Done determining best model");
		logger.info("Generating Decision Viz Data...");

		Map<String, Object> vizData = new HashMap<String, Object>();
		vizData.put("name", "Decision Tree For " + retHeaders[0]);
		vizData.put("layout", "Dendrogram");
		vizData.put("panelId", getPanelId());
		// add the actual data
		Map<String, Map> classificationMap = processTreeString(treeAsString);
		vizData.put("children", classificationMap);
		// add the accuracy and precision
		DecimalFormat df = new DecimalFormat("#%");
		List<Map<String, String>> statList = new ArrayList<Map<String, String>>();
		Map<String, String> statHash = new HashMap<String, String>();
		statHash.put("Accuracy", df.format(accuracy/100));
		statList.add(statHash);
		statHash = new Hashtable<String, String>();
		statHash.put("Precision", df.format(precision));
		statList.add(statHash);
		vizData.put("stats", statList);

		// now return this object
		return new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
	}
	
	private int getNumRows(ITableDataFrame frame, QueryColumnSelector predictorCol) {
		QueryStruct2 qs = new QueryStruct2();
		QueryMathSelector math = new QueryMathSelector();
		math.setInnerSelector(predictorCol);
		math.setMath(QueryAggregationEnum.COUNT);
		qs.addSelector(math);
		
		Iterator<IHeadersDataRow> countIt = frame.query(qs);
		while(countIt.hasNext()) {
			return ((Number) countIt.next().getValues()[0]).intValue();
		}
		return 0;
	}
	
	private Instances[][] crossValidationSplit(Instances data, int numberOfFolds) {
		Instances[][] split = new Instances[2][numberOfFolds];
		for (int i = 0; i < numberOfFolds; i++) {
			split[0][i] = data.trainCV(numberOfFolds, i);
			split[1][i] = data.testCV(numberOfFolds, i);
		}
		return split;
	}
	
	private Evaluation classify(Classifier model, Instances trainingSet, Instances testingSet) throws Exception {
		Evaluation evaluation = new Evaluation(trainingSet);
		model.buildClassifier(trainingSet);
		evaluation.evaluateModel(model, testingSet);
		return evaluation;
	}
	
	private Map<String, Map> processTreeString(String treeAsString) {
		String[] treeSplit = treeAsString.split("\n");
		Map<String, Map> treeMap = new HashMap<String, Map>();
		// exception case when tree is a single node
		if(treeSplit.length == 7 && treeSplit[6].equals("Size of the tree : 	1")) {
			generateNodeTreeWithParenthesis(treeMap, treeSplit[2]);
		} else {
			String[] treeStringArr = new String[treeSplit.length - 7];
			// indices based on weka J48 decision tree output
			System.arraycopy(treeSplit, 3, treeStringArr, 0, treeStringArr.length);
			generateTreeEndingWithParenthesis(treeMap, "", 0, treeStringArr, new Integer(0));
		}
		return treeMap;
	}
	
	private void generateNodeTreeWithParenthesis(Map<String, Map> rootMap, String nodeValue) {
		String lastRegex = "(\\(\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+\\|\\d+\\.\\d+/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d/\\d+\\.\\d+\\))|(\\(\\d+\\.\\d+/\\d+\\.\\d+\\|\\d+\\.\\d+\\))";

		String key = nodeValue.replaceFirst(":", "").replaceFirst(lastRegex, "").trim();
		rootMap.put(key, new HashMap<String, Map>());
	}
	
	private void generateTreeEndingWithParenthesis(Map<String, Map> rootMap, String startKey, int subTreeIndex, String[] treeStringArr, Integer index) {
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
					currTree = new HashMap<String, Map>();
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
				generateTreeEndingWithParenthesis(currTree, newKey, newSubTreeIndex, treeStringArr, index);
			}
		}
	}

	
	////////////////////////////////////////////////////////////////
	
	/*
	 * Get input values for algorithm
	 */

	private String getClassificationColumn(Logger logger) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(CLASSIFICATION_COLUMN);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		
		// else, we assume it is the first column
		if(this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find the column predict";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}
	
	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ATTRIBUTES);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
		
		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for(Object obj : values) {
			strValues.add(obj.toString());
		}
		// but we remove the first index as it is assumed to be the predictor
		strValues.remove(0);
		return strValues;
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(PANEL);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}
}