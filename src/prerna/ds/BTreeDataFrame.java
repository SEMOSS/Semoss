package prerna.ds;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.ISelectStatement;

public class BTreeDataFrame implements ITableDataFrame {

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

			if (level==null) {
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
	public void addRow(Object[] rowData) {
		if(levelNames.length != rowData.length) {
			throw new IllegalArgumentException("The input rowData must have the same length as the current number of levels in the tree");
		}
		
		// get parent node
		ISEMOSSNode parent = createNodeObject(rowData[0], null, levelNames[0]);
		
		// if no children nodes found, add node by itself
		if(rowData.length == 1) {
			simpleTree.createNode(parent, false);
			return;
		}
		
		// if children nodes found, add each parent-child relationship to tree
		for(int i = 1; i < rowData.length; i++) {
			ISEMOSSNode child = createNodeObject(rowData[i], null, levelNames[i]);
			
			simpleTree.addNode(parent, child);
			parent = child;
		}
	}
	
	
	private ISEMOSSNode createNodeObject(Object value, String rawValue, String level) {
		ISEMOSSNode node;
		if(value == null){
			node = new StringClass(null, level);
		} 
		else if(value instanceof String){
			node = new StringClass((String)value, level);
		} 
		//else if(value instanceof Number) {
		//child = new DoubleClass((double)value, level);
		//} 
		//else if(value instanceof Boolean) {
		//child = new BooleanClass((boolean)value, level);
		//} 
		else{
			node = new StringClass(null, level);
		}
		return node;
	}

	@Override
	public ArrayList<Object[]> getData() {
		if(simpleTree == null) {
			return null;
		}

		Vector<String> levels = simpleTree.findLevels();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levels.elementAt(0));
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		ArrayList<Object[]> table = new ArrayList<Object[]>();
		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<String>(), table, levels.size());

		return table;
	}

	@Override
	public Vector<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumCols() {
		// TODO Auto-generated method stub
		return 0;
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
}
