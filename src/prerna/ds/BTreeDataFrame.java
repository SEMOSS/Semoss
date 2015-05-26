package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
		while(parent == null) { //Do we need this while loop? We should create a null parent if thats what the parent is
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
		} else if(value instanceof Integer) {
			node = new IntClass((int)value, level);
		} else if(value instanceof Number) {
			node = new DoubleClass((double)value, level);
		} else if(value instanceof String) {
			node = new StringClass((String)value, level);
		}
//		else if(value instanceof Boolean) {
//			node = new BooleanClass((boolean)value, level);
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
		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<Object>(), table, levelNames.length);

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
		List<SEMOSSParam> params = routine.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(params.get(0).getName(), colNameInTable);
		selectedOptions.put(params.get(1).getName(), colNameInJoiningTable);
		//if the routine takes in confidence threshold, must pass that in as well
		if(params.size()>2){
			selectedOptions.put(params.get(2).getName(), confidenceThreshold);
		}
		routine.setSelectedOptions(selectedOptions);

		// let the routine run
		LOGGER.info("Begining matching routine");
		ITableDataFrame matched = routine.runAlgorithm(this, table);

		if(table instanceof BTreeDataFrame){
			BTreeDataFrame passedTree = (BTreeDataFrame) table;
			
			//Here is the logic that this should use.
			// Iterate for every row in the matched table
			// search for tree node in this table and tree node in passed table
			// hook up passed tree node with each instance of this tree node
			// make deep copy of passed tree node if there is more than one instance on this tree node
			///// I also probably need to make sure types aren't duplicated (more than one col with same type) or is this not a problem...?
			///// Also need to grab index trees of passed table and add to my index hash

			String[] columnNames = passedTree.getColumnHeaders();
			// add the new data to this tree
			LOGGER.info("Augmenting tree");
			joinTreeLevels(columnNames, colNameInJoiningTable); // need to add new levels to this tree's level array
			List<Object[]> flatMatched = matched.getData();// TODO: this could be replaced with nextRow or getRow method directly on the tree

			// Iterate for every row in the matched table
			for(Object[] flatMatchedRow : flatMatched) { // for each matched item
				Object item1 = flatMatchedRow[0];
				Object item2 = flatMatchedRow[1];

				// search for tree node in this table and tree node in passed table
				TreeNode thisRootNode = this.simpleTree.nodeIndexHash.get(colNameInJoiningTable); //TODO: is there a better way to get the type? I don't think this is reliable
				TreeNode thisSearchNode = new TreeNode(createNodeObject(item1, null, colNameInJoiningTable)); //TODO: how do we generically do this...?
				Vector thisSearchVector = new Vector();
				thisSearchVector.addElement(thisRootNode);
				TreeNode thisTreeNode = thisRootNode.getNode(thisSearchVector, thisSearchNode, false);
				Vector <SimpleTreeNode> thisInstances = thisTreeNode.getInstances();
				System.out.println(thisInstances.size());
				
				SimpleTreeBuilder passedBuilder = passedTree.getBuilder();
				TreeNode passedRootNode = passedBuilder.nodeIndexHash.get(colNameInJoiningTable); //TODO: is there a better way to get the type? I don't think this is reliable
				TreeNode passedSearchNode = new TreeNode(createNodeObject(item2, null, colNameInJoiningTable)); //TODO: how do we generically do this...?
				Vector passedSearchVector = new Vector();
				passedSearchVector.addElement(passedRootNode);
				TreeNode passedTreeNode = passedRootNode.getNode(passedSearchVector, passedSearchNode, false);
				Vector <SimpleTreeNode> passedInstances = passedTreeNode.getInstances();
				System.out.println(passedInstances.size()); // this should be 1
				SimpleTreeNode instance2HookUp = passedInstances.get(0);

				// hook up passed tree node with each instance of this tree node
				for(int instIdx = 0; instIdx < thisInstances.size(); instIdx++){
					SimpleTreeNode hookUp = instance2HookUp;
					if(instIdx != thisInstances.size() - 1){ // unless this is the last one, we need to make a deep copy
						// make a deep copy of instance2HookUp
//						hookUp = new SimpleTreeNode(instance2HookUp);
					}
				}
			}
			
			
		}
		else // use the flat join. This is not idea. Not sure if we will ever actually use this
		{
			String[] columnNames = table.getColumnHeaders();
			
			// add the new data to this tree
			LOGGER.info("Augmenting tree");
			joinTreeLevels(columnNames, colNameInJoiningTable);
			List<Object[]> flatMatched = matched.getData();
			List<Object[]> flatTable = table.getData();
			// loop through all rows
			// TODO: this is terrible logic that is extremely inefficient. If we want to join something that is not a btree, revist this for sure
			for(Object[] flatMatchedRow : flatMatched) { // for each matched item
				// add row as a key-value pair of level to instance value
				Object item1 = flatMatchedRow[0];
				Object item2 = flatMatchedRow[1];
				for(Object[] flatRow : flatTable){
					if(flatRow[0].equals(item2)) // get the whole row associated with that matched item
					{
						Map<String, Object> row = new HashMap<String, Object>();
						for(int i = 0; i < columnNames.length; i++) {
							String colName = columnNames[i];
							if(colName.equals(colNameInJoiningTable)){ // fill in the row replacing the matched on the right value with the matched on the left value
								row.put(colNameInTable, item1);
							}
							else {
								row.put(colName, flatRow[i]);
							}
						}
						this.addRow(row);
					}
				}
			}
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
	
	/***
	 * 
	 * @param table - the table to be appended to this table
	 * @param levels - the order in which levels will be extracted from table
	 * 
	 * Append
	 * 	The method will take the values from 'table' row by row from the columns specified in 'levels' in the order listed in 'levels'
	 * 	and add the values to this table
	 * 
	 * Assumptions
	 * no new columns will be added to this table, therefore 'List<String> levels' must be a subset of this table's levels
	 * */
	public void append(ITableDataFrame table, List<String> levels) {
		//create an array so that it is known where is level is located in the table
		int columnLength = levels.size();
		String[] tableStructure = table.getColumnHeaders();
		int[] orderArray = new int[columnLength];
		
		for (int i = 0; i < columnLength; i++) {
			int j = 0;
			for(; j < tableStructure.length; j++){
				if(levels.get(i).equals(tableStructure[j])) break;
			}
			orderArray[i] = j;
		}
		
		ISEMOSSNode parent = null;
		ISEMOSSNode child = null;
		
		int numRows = table.getNumRows();
		List<Object[]> flatTable = table.getData();
		Object[] row;
		
		//go row by row and add the parent-child relationships to this table
		for(int i = 0; i < numRows; i++) {
			row = flatTable.get(i);
			parent = createNodeObject(row[orderArray[0]], null, levels.get(i));
			//parent = (ISEMOSSNode)row[orderArray[0]];
			
			if(columnLength == 1) {
				simpleTree.createNode(parent, false);
			} 
			else {
				for(int j = 1; i < row.length; i++) {
					child = createNodeObject(row[orderArray[j]], null, levels.get(j));
					//child = (ISEMOSSNode)row[orderArray[j]];
					simpleTree.addNode(parent, child);
					parent = child;
				}
			}
		}
	}
	
	public void append(ITableDataFrame table, Map<String, String> tableLookup) {
		List<String> levels = new ArrayList<>();
		for(int i = 0; i < levelNames.length; i ++) {
			if(tableLookup.containsKey(levelNames[i])) levels.add(tableLookup.get(levelNames[i]));
		}
		
		this.append(table, levels);
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
		if(!ArrayUtilityMethods.arrayContainsValue(levelNames, columnHeader)) {
			throw new IllegalArgumentException("Column name is not contained in the table");
		}
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		ITreeKeyEvaluatable nodeEvaluator = typeRoot.getInstances().elementAt(0).leaf;
		
		if(nodeEvaluator instanceof IntClass || nodeEvaluator instanceof DoubleClass) {
			return true;
		}
		
		return false;
	}

	@Override
	public boolean[] isNumeric() {
		boolean[] isNumeric = new boolean[levelNames.length];
		
		for(int i = 0; i < levelNames.length; i++) {
			isNumeric[i] = isNumeric(levelNames[i]);
		}
		
		return isNumeric;
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
	public Iterator<Object[]> iterator() {
		Iterator<Object[]> it = new BTreeIterator(simpleTree.getInstances(levelNames[levelNames.length-1]));
		return it;
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		typeRoot = typeRoot.getLeft(typeRoot);
		List<Object> table = typeRoot.flattenToArray(typeRoot, true);

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

	public static void main(String[] args) {
		//use this as a test method
	}
	
	public SimpleTreeBuilder getBuilder(){
		return this.simpleTree;
	}
}
