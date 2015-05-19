package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.ISelectStatement;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;

public class BTreeDataFrame implements ITableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(BTreeDataFrame.class.getName());
	private SimpleTreeBuilder simpleTree;
	private String[] levelNames;

	public BTreeDataFrame(String[] levelNames) {
		this.simpleTree = new SimpleTreeBuilder();
		this.levelNames = levelNames;
	}

	@Override
	public void addRow(ISelectStatement rowData) {
		Hashtable rowHash = rowData.getPropHash(); //cleaned data
		Hashtable rowRawHash = rowData.getRPropHash(); //raw data
		Set<String> rowKeys = rowHash.keySet(); //these are the simple tree types or column names in the table

		Vector<String> levels = simpleTree.findLevels();
		Vector<String> rowOrder = new Vector<>();

		for(String level: levels) {
			if(rowKeys.contains(level)) {
				rowOrder.add(level);
			} else {
				rowOrder.add(null);
			}
		}

		//Add the keys that are new to the levels
		for(String key: rowKeys){
			if(!rowOrder.contains(key)){
				rowOrder.add(key);
			}
		}

		//How do you create an empty node?
		ISEMOSSNode child;
		Object value;
		String rawValue;

		String level = rowOrder.get(0);
		value = (level==null) ? null : rowHash.get(level);
		rawValue = (String) ((level==null) ? null : rowRawHash.get(level));

		ISEMOSSNode parent = createNodeObject(value, rawValue, level);

		for(int i = 1; i<rowOrder.size(); i++) {
			level = rowOrder.get(i);

			if(level == null) {
				value = null;
				rawValue = null;
			} else {
				value = rowHash.get(level);
				rawValue = (String) rowRawHash.get(level);
			}

			child = createNodeObject(value, rawValue, level);
			simpleTree.addNode(parent, child);
			parent = child;
		}
	}

	@Override
	public void addRow(Map<String, Object> rowData) {
		// keys that are not in current tree level will not be used
		for(String key : rowData.keySet()) {
			if(!ArrayUtilityMethods.arrayContainsValue(levelNames, key)) {
				LOGGER.error("Column name " + key + " does not exist in current tree");
			}
		}

		ISEMOSSNode parent = null;
		int index = 0;
		while(parent == null) {
			Object val = rowData.get(levelNames[index]);
			if(val != null) {
				//TODO: how to deal with URI being null
				parent = createNodeObject(val, null, levelNames[index]);
			}
			if(index == levelNames.length-1) {
				break;
			}
			index++;
		}

		if(parent == null) {
			LOGGER.error("No information found to add to data frame");
			return;
		}

		boolean foundChild = false;
		ISEMOSSNode child;
		for(; index < levelNames.length; index++) {
			Object val = rowData.get(levelNames[index]);
			if(val == null) {
				continue;
			} else {
				foundChild = true;
				//TODO: how to deal with URI being null
				child = createNodeObject(val, null, levelNames[index]);

				simpleTree.addNode(parent, child);
				parent = child;
			}

		}

		// not one relationship found, add an empty node
		if(!foundChild) {
			simpleTree.createNode(parent, false);
		}
	}


	private ISEMOSSNode createNodeObject(Object value, String rawValue, String level) {
		ISEMOSSNode node;
		if(value == null) {
			node = new StringClass(null, level);
		} 
		else if(value instanceof String) {
			node = new StringClass((String)value, level);
		} 
		//else if(value instanceof Number) {
		//child = new DoubleClass((double)value, level);
		//} 
		//else if(value instanceof Boolean) {
		//child = new BooleanClass((boolean)value, level);
		//} 
		else {
			node = new StringClass(null, level);
		}
		return node;
	}

	@Override
	public ArrayList<Object[]> getData() {
		if(simpleTree == null) {
			return null;
		}

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		ArrayList<Object[]> table = new ArrayList<Object[]>();
		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<String>(), table, levelNames.length);

		return table;
	}

	@Override
	public Vector<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IAnalyticRoutine routine) {
		LOGGER.info("Begining join on columns ::: " + colNameInTable + " and " + colNameInJoiningTable);
		LOGGER.info("Confidence Threshold :: " + confidenceThreshold);
		LOGGER.info("Analytics Routine ::: " + routine.getName());

		// fill the options needed for the routine
		List<SEMOSSParam> params = routine.getAllAlgorithmOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(params.get(0).getName(), colNameInTable);
		selectedOptions.put(params.get(1).getName(), colNameInJoiningTable);
		//if the routine takes in confidence threshold, must pass that in as well
		if(params.size()>2){
			selectedOptions.put(params.get(2).getName(), confidenceThreshold);
		}
		routine.setOptions(selectedOptions);

		// let the routine run
		LOGGER.info("Begining matching routine");
		ITableDataFrame matched = routine.runAlgorithm(this, table);
		Vector<String> columnNames = matched.getColumnHeaders();
		
		// add the new data to this tree
		LOGGER.info("Augmenting tree");
		joinTreeLevels(columnNames, colNameInJoiningTable);
		ArrayList<Object[]> flatMatched = matched.getData();
		// loop through all rows
		for(Object[] flatRow : flatMatched) {
			// add row as a key-value pair of level to instance value
			Map<String, Object> row = new HashMap<String, Object>();
			for(int i = 0; i < columnNames.size(); i++) {
				row.put(columnNames.elementAt(i), flatRow[i]);
			}
			this.addRow(row);
		}
	}

	private void joinTreeLevels(Vector<String> joinLevelNames, String colNameInJoiningTable) {
		String[] newLevelNames = new String[levelNames.length + joinLevelNames.size() - 1];
		// copy old values to new
		System.arraycopy(levelNames, 0, newLevelNames, 0, levelNames.length);
		for(int i = levelNames.length; i < newLevelNames.length + 1; i++) {
			String name = joinLevelNames.elementAt(i-levelNames.length);
			if(name.equals(colNameInJoiningTable)) {
				//skip this since the column is being joined
			} else {
				newLevelNames[i] = joinLevelNames.elementAt(i-levelNames.length);
			}
		}

		this.levelNames = newLevelNames;
	}

	@Override
	public void undoJoin() {
		// TODO Auto-generated method stub
	}

	@Override
	public void append(ITableDataFrame table) {
		// TODO Auto-generated method stub
	}

	@Override
	public void undoAppend() {
		// TODO Auto-generated method stub
	}

	@Override
	public void performAction(IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
	}

	@Override
	public void undoAction() {
		// TODO Auto-generated method stub
	}

	@Override
	public Double getEntropy(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getEntropy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getEntropyDensity(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getEntropyDensity() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer[] getUniqueInstanceCount() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getMax() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getMin() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getAverage(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getAverage() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getSum(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getSum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean[] isNumeric() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getColumnHeaders() {
		Vector<String> retVec = new Vector<String>();
		for(int i = 0; i < levelNames.length; i++) {
			retVec.insertElementAt(levelNames[i], i);
		}
		return retVec;
	}

	@Override
	public int getNumCols() {
		return levelNames.length;
	}

	@Override
	public int getNumRows() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getColCount(int rowIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getRowCount(String columnHeader) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object[] getRow(int rowIdx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getUniqueValues(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Integer> getUniqueValuesAndCount(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void refresh() {
		// TODO Auto-generated method stub
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		// TODO Auto-generated method stub
	}

	@Override
	public void removeColumn(String columnHeader) {
		// TODO Auto-generated method stub
	}

	@Override
	public void removeRow(int rowIdx) {
		// TODO Auto-generated method stub
	}

	@Override
	public ITableDataFrame[] splitTableByColumn(String colHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITableDataFrame[] splitTableByRow(int rowIdx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unfilter(String columnHeader) {
		// TODO Auto-generated method stub
	}

	public String[] getTreeLevels() {
		return this.levelNames;
	}
}
