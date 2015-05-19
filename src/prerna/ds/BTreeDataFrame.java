package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
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

	public BTreeDataFrame() {
		this.simpleTree = new SimpleTreeBuilder();
		levelNames = new String[1]; //{"Director"};
		levelNames[0] = "Director";
	}
	public BTreeDataFrame(String[] levelNames) {
		this.simpleTree = new SimpleTreeBuilder();
		this.levelNames = levelNames;
	}

	@Override
	public void addRow(ISelectStatement rowData) {
		//Map rowHash = rowData.getPropHash(); //cleaned data
		//Map rowRawHash = rowData.getRPropHash(); //raw data
		addRow(rowData.getPropHash());
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
//		else if(value instanceof Number) {
//			child = new DoubleClass((double)value, level);
//		} 
//		else if(value instanceof Boolean) {
//			child = new BooleanClass((boolean)value, level);
//		} 
		else {
			node = new StringClass(null, level);
		}
		return node;
	}
	
	@Override
	public List<Object[]> getData() {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		List<Object[]> table = new ArrayList<Object[]>();
		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<String>(), table, levelNames.length);

		return table;
	}

	@Override
	public List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IAnalyticRoutine routine) {
		LOGGER.info("Columns Passed ::: " + colNameInTable + " and " + colNameInJoiningTable);
		LOGGER.info("Confidence Threshold :: " + confidenceThreshold);
		LOGGER.info("Analytics Routine ::: " + routine.getName());
		
		
		
		LOGGER.info("Begining join on columns ::: " + colNameInTable + " and " + colNameInJoiningTable);

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
		String[] columnNames = matched.getColumnHeaders();
		
		// add the new data to this tree
		LOGGER.info("Augmenting tree");
		joinTreeLevels(columnNames, colNameInJoiningTable);
		List<Object[]> flatMatched = matched.getData();
		// loop through all rows
		for(Object[] flatRow : flatMatched) {
			// add row as a key-value pair of level to instance value
			Map<String, Object> row = new HashMap<String, Object>();
			for(int i = 0; i < columnNames.length; i++) {
				row.put(columnNames[i], flatRow[i]);
			}
			this.addRow(row);
		}
	}

	private void joinTreeLevels(String[] joinLevelNames, String colNameInJoiningTable) {
		String[] newLevelNames = new String[this.levelNames.length + joinLevelNames.length - 1];
		// copy old values to new
		System.arraycopy(levelNames, 0, newLevelNames, 0, levelNames.length);
		int newNameIdx = levelNames.length;
		for(int i = 0; i < joinLevelNames.length; i++) {
			String name = joinLevelNames[i];
			if(name.equals(colNameInJoiningTable)) {
				//skip this since the column is being joined
			} else {
				newLevelNames[newNameIdx] = joinLevelNames[i];
				newNameIdx ++;
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
	public String[] getColumnHeaders() {
		return this.levelNames;
	}

	@Override
	public int getNumCols() {
		return this.levelNames.length;
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

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		typeRoot = typeRoot.getLeft(typeRoot);

		List<String> table = typeRoot.flattenToArray(typeRoot, true);
		

		System.out.println("Final count for column " + columnHeader + " = " + table.size());
		return table.toArray();
		
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
