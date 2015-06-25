package prerna.ds;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.impl.ExactStringMatcher;
import prerna.engine.api.ISelectStatement;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class BTreeDataFrame implements ITableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(BTreeDataFrame.class.getName());
	private SimpleTreeBuilder simpleTree;
	private String[] levelNames;
	private static int testnum=0;

	public BTreeDataFrame() {
		this.simpleTree = new SimpleTreeBuilder();
		
		//This was used for testing purposes
		//levelNames = new String[1]; //{"Director"};
		//levelNames[0] = "Director";
	}
	public BTreeDataFrame(String[] levelNames) {
		this.simpleTree = new SimpleTreeBuilder();
		this.levelNames = levelNames;
	}

	@Override
	public void addRow(ISelectStatement rowData) {
		addRow(rowData.getPropHash(), rowData.getRPropHash());
	}

	@Override
	public void addRow(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {
		// keys that are not in current tree level will not be used
		for(String key : rowCleanData.keySet()) {
			if(!ArrayUtilityMethods.arrayContainsValue(levelNames, key)) {
				LOGGER.error("Column name " + key + " does not exist in current tree");
			}
		}
		
		ISEMOSSNode[] row = new ISEMOSSNode[levelNames.length];
		for(int index = 0; index < levelNames.length; index++) {
			Object val = rowCleanData.get(levelNames[index]);
			Object rawVal = rowRawData.get(levelNames[index]);
			//TODO: better way of doing this????
			if(val.toString().isEmpty()) {
				val = SimpleTreeNode.EMPTY;
			}
			if(rawVal.toString().isEmpty()) {
				rawVal = SimpleTreeNode.EMPTY;
			}
			row[index] = createNodeObject(val, rawVal, levelNames[index]);

		}
		simpleTree.addNodeArray(row);
	}

	private ISEMOSSNode createNodeObject(Object value, Object rawValue, String level) {
		ISEMOSSNode node;

		if(value == null) {
			node = new StringClass(null, level); // TODO: fix this
		} else if(value instanceof Integer) {
			node = new IntClass((int)value, (int)value, level);
		} else if(value instanceof Number) {
			node = new DoubleClass((double)value, (double)value, level);
		} else if(value instanceof String) {
			node = new StringClass((String)value, (String) rawValue.toString(), level);
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
		List<Object[]> table = new ArrayList<Object[]>();
		
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		//TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);
		if(typeRoot == null){
			LOGGER.info("Table is empty............................");
			return table;
		}
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<Object>(), table, levelNames.length);
		
		return table;
	}
	
	@Override
	public List<Object[]> getRawData() {
		List<Object[]> table = new ArrayList<Object[]>();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		if(typeRoot == null){
			LOGGER.info("Table is empty............................");
			return table;
		}
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		leftRootNode.flattenRawTreeFromRoot(leftRootNode, new Vector<Object>(), table, levelNames.length);

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
			int origLength = this.levelNames.length;
			String[] newLevels = joinTreeLevels(columnNames, colNameInJoiningTable); // need to add new levels to this tree's level array
			List<Object[]> flatMatched = matched.getData();// TODO: this could be replaced with nextRow or getRow method directly on the tree

			TreeNode thisRootNode = this.simpleTree.nodeIndexHash.get(colNameInTable); //TODO: is there a better way to get the type? I don't think this is reliable
			//this.simpleTree.adjustType(colNameInTable, true);

			Vector thisSearchVector = new Vector();
			thisSearchVector.addElement(thisRootNode);

			SimpleTreeBuilder passedBuilder = passedTree.getBuilder();
			Map<String, TreeNode> newIdxHash2 = new HashMap<String, TreeNode>();
			TreeNode passedRootNode = passedBuilder.nodeIndexHash.remove(colNameInJoiningTable); //TODO: is there a better way to get the type? I don't think this is reliable
			Vector passedSearchVector = new Vector();
			passedSearchVector.addElement(passedRootNode);

			// Iterate for every row in the matched table
			for(Object[] flatMatchedRow : flatMatched) { // for each matched item
				Object item1 = flatMatchedRow[0];
				Object item2 = flatMatchedRow[1];
//				System.out.println(item1 + "           " + item2);
				
				// search for tree node in this table and tree node in passed table
				TreeNode thisSearchNode = new TreeNode(createNodeObject(item1, item1, colNameInTable)); //TODO: how do we generically do this...?
				TreeNode thisTreeNode = thisRootNode.getNode(thisSearchVector, thisSearchNode, false);
				
				TreeNode passedSearchNode = new TreeNode(createNodeObject(item2, item2, colNameInJoiningTable)); //TODO: how do we generically do this...?
				TreeNode passedTreeNode = passedRootNode.getNode(passedSearchVector, passedSearchNode, false);
				
				if(item1.equals(SimpleTreeNode.EMPTY) && item2.equals(SimpleTreeNode.EMPTY)) {
					continue;
				}
				
				if(item1.equals(SimpleTreeNode.EMPTY)) {
					/*
					 * Logic is to create (or find) a trail of empty nodes from the root and then connect to the values of item2
					 */
					Vector <SimpleTreeNode> passedInstances = passedTreeNode.getInstances();
					
					// see if empty node is at root
					int currLevel = 0;
					ITreeKeyEvaluatable emptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, levelNames[currLevel]);
					TreeNode emptyNode = new TreeNode(emptyVal);
					
					TreeNode rootOfRoots = this.simpleTree.nodeIndexHash.get(levelNames[currLevel]);
					Vector<TreeNode> searchForEmpty = new Vector<TreeNode>();
					searchForEmpty.add(rootOfRoots);
					TreeNode foundEmpty = rootOfRoots.getNode(searchForEmpty, emptyNode, false);
					
					SimpleTreeNode joiningNode = null;
					// found empty node at root
					if(foundEmpty != null) {
						currLevel++;
						// loop through instances and see if any children are blanks
						Vector<SimpleTreeNode> emptyInstances = foundEmpty.getInstances();
						for(int i = 0; i < emptyInstances.size(); i++) {
							joiningNode = findLastConnectedEmptyNode(emptyInstances.get(i), currLevel);
							if(joiningNode != null) {
								break;
							}
						}
						String type = ((ISEMOSSNode) joiningNode.leaf).getType();
						currLevel = ArrayUtilityMethods.arrayContainsValueAtIndex(levelNames, type);
					} 
					else { // found no empty nodes, need to construct trail from root
						// define first level node
						joiningNode = new SimpleTreeNode(emptyVal);
						
						// connect via value true
						SimpleTreeNode firstInstance = rootOfRoots.getInstances().get(0);
						SimpleTreeNode previousLeftMost = firstInstance.getLeft(firstInstance);
						previousLeftMost.leftSibling = joiningNode;
						joiningNode.rightSibling = previousLeftMost;
						
						// connect via index tree
						emptyNode.getInstances().add(joiningNode);
						TreeNode root = rootOfRoots.insertData(emptyNode);
						this.simpleTree.nodeIndexHash.put(levelNames[currLevel], root);
					}
					
					currLevel++;
					// build tree to desired length
					for(int i = currLevel; i < origLength; i++) {
						emptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, levelNames[i]);
						emptyNode = new TreeNode(emptyVal);
						SimpleTreeNode newEmpty = new SimpleTreeNode(emptyVal);
						emptyNode.getInstances().add(newEmpty);

						if(joiningNode.leftChild == null) {
							joiningNode.leftChild = newEmpty;
							newEmpty.parent = joiningNode;
						} else {
							SimpleTreeNode joiningChild = joiningNode.leftChild;
							// adjust sibling rel
							joiningChild.leftSibling = newEmpty;
							newEmpty.rightSibling = joiningChild;
							// adjust parent child rel
							joiningNode.leftChild = newEmpty;
							newEmpty.parent = joiningNode;
						}
						joiningNode = newEmpty;
						
						// update index tree
						TreeNode previousRoot = this.simpleTree.nodeIndexHash.get(levelNames[i]);
						TreeNode newRoot = previousRoot.insertData(emptyNode);
						this.simpleTree.nodeIndexHash.put(levelNames[i], newRoot);
					}
					
					SimpleTreeNode instance2HookUp = passedInstances.get(0).leftChild;
					Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
					vec.add(instance2HookUp);
					String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
					SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
					SimpleTreeNode.addLeafChild(joiningNode, hookUp);
					
				} else if(item2.equals(SimpleTreeNode.EMPTY)) {
					/*
					 * Logic is for the item1 node, to add a trail of empty nodes after it equal to the length of what would be added
					 */
					
					Vector <SimpleTreeNode> thisInstances = thisTreeNode.getInstances();
					
					ITreeKeyEvaluatable emptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, columnNames[1]);
					SimpleTreeNode instance2HookUp = new SimpleTreeNode(emptyVal);
					SimpleTreeNode dummy = instance2HookUp;
					int i = 1;
					while(i < columnNames.length-1) {
						ITreeKeyEvaluatable newEmptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, columnNames[i+1]);
						SimpleTreeNode newEmpty = new SimpleTreeNode(newEmptyVal);
						dummy.leftChild = newEmpty;
						newEmpty.parent = dummy;
						dummy = newEmpty;
						i++;
					}

					Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
					vec.add(instance2HookUp);
					String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
					
					for(int instIdx = 0; instIdx < thisInstances.size(); instIdx++){
						SimpleTreeNode myNode = thisInstances.get(instIdx);
						SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
						SimpleTreeNode.addLeafChild(myNode, hookUp);
					}
					
				} else {
					Vector <SimpleTreeNode> thisInstances = thisTreeNode.getInstances();
					Vector <SimpleTreeNode> passedInstances = passedTreeNode.getInstances();

					SimpleTreeNode instance2HookUp = passedInstances.get(0).leftChild;
					Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
					vec.add(instance2HookUp);
					String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
//					System.out.println("SERIALIZED " + instance2HookUp.leaf.getKey() + " AS " + serialized);
						
					// hook up passed tree node with each instance of this tree node
					for(int instIdx = 0; instIdx < thisInstances.size(); instIdx++){
						SimpleTreeNode myNode = thisInstances.get(instIdx);
						SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
						SimpleTreeNode.addLeafChild(myNode, hookUp);
					}
				}
			}
			
			this.simpleTree.adjustType(levelNames[origLength - 1], true);
			
			//Update the Index Tree
			TreeNode treeRoot = this.simpleTree.nodeIndexHash.get(colNameInTable);
			ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(treeRoot);
			while(iterator.hasNext()) {
				SimpleTreeNode t = iterator.next();
				this.simpleTree.appendToIndexTree(t.leftChild);
			}

		}//EMPTY
		else // use the flat join. This is not ideal. Not sure if we will ever actually use this
		{
			//TODO: CURRENTLY ADDING THE RAW VALUES IN FLATTENED-TABLE AS BOTH RAW AND CLEAN DATA IN JOIN

			String[] columnNames = table.getColumnHeaders();
			// add the new data to this tree
			LOGGER.info("Augmenting tree");
			joinTreeLevels(columnNames, colNameInJoiningTable);
			List<Object[]> flatMatched = matched.getRawData();
			List<Object[]> flatTable = table.getRawData();
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
						this.addRow(row, row);
					}
				}
			}
		}
	}

	private SimpleTreeNode findLastConnectedEmptyNode(SimpleTreeNode emptyNode, int currLevel) {
		StringClass emptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, levelNames[currLevel]);
		SimpleTreeNode joiningNode = emptyNode.leftChild;
		
		while(joiningNode != null && !joiningNode.leaf.isEqual(emptyVal)) {
			joiningNode = joiningNode.rightSibling;
		}
		
		if(joiningNode == null) {
			return emptyNode;
		} else {
			SimpleTreeNode retNode = findLastConnectedEmptyNode(joiningNode, ++currLevel);
			return retNode;
		}
	}
	
    private String[] joinTreeLevels(String[] joinLevelNames, String colNameInJoiningTable) {
        String[] newLevelNames = new String[this.levelNames.length + joinLevelNames.length - 1];
        // copy old values to new
        System.arraycopy(levelNames, 0, newLevelNames, 0, levelNames.length);
        int newNameIdx = 0;
        String[] onlyNewNames = new String[joinLevelNames.length - 1];
        for(int i = 0; i < joinLevelNames.length; i++) {
               String name = joinLevelNames[i];
               if(name.equals(colNameInJoiningTable)) {
                     //skip this since the column is being joined
               } else {
                     newLevelNames[newNameIdx + levelNames.length] = joinLevelNames[i];
                     onlyNewNames[newNameIdx] = joinLevelNames[i];
                     newNameIdx ++;
               }
        }

        this.levelNames = newLevelNames;
        return onlyNewNames;
 }


	@Override
	public void undoJoin() {
		// TODO Auto-generated method stub
	}

	@Override
	public void append(ITableDataFrame table) {

	}
	
	private void append(BTreeDataFrame bTree) {
		/*
		 * Algorithm
		 * 
		 * Determine the levels of the incoming tree
		 * if the levels are the same (type and order)
		 * 		compare the roots of the tree
		 * 		Simply Add different roots to the root level
		 * 		for the same roots
		 * 			repeat the process of each child on the next level
		 * if the levels are not the same
		 * 		Case 1: same types, different order
		 * 		Case 2: appending
		 * 
		 * 
		 * */
		int levelCase = 1;
		//Determine how this BTreeDataFrame's structure compares with bTree argument's structure
		String[] tableHeaders = bTree.getColumnHeaders();
		if(tableHeaders.length == levelNames.length) {
			for(int i = 0; i < levelNames.length; i++) {
				if(!levelNames[i].equals(tableHeaders[i])) {
					levelCase = 0; break;
				}
			}
		} else {
			//find which if any columns match the table
			//should this use IAnalyticRoutine?
		} 
		
		switch(levelCase) {
			case 1: {
				//structurally identical
				simpleTree.append(this.simpleTree.getRoot(), bTree.getTree().getRoot()); break;
			}
			case 2: {
				//subset of what to append is structurally identical
				//find the top level of what needs to append, append each 'subTree' in a for loop
				break;
			}
			case 3: {
				//what to append is full but jumbled version of structure to append to
				break;
			}
			case 4: {
				//what to append is a jumbled subset of the structure
				break;
			}
			default: {
				//completely different, can't append
				break;
			}
		}
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
	public Iterator<Object[]> iterator() {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);	
//		typeRoot = typeRoot.getLeft(typeRoot);
		Iterator<Object[]> it = new BTreeIterator(typeRoot);
		return it;
	}

	@Override
	public Object[] getColumn(String columnHeader) {

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		if(typeRoot == null){ // TODO this null check shouldn't be needed. When we join, we need to add empty nodes--need to call balance at somepoint or something like that
			LOGGER.info("Table is empty............................");
			return new Object[0];
		}
		typeRoot = typeRoot.getLeft(typeRoot);
		List<Object> table = typeRoot.flattenToArray(typeRoot, true);
		

		System.out.println("Final count for column " + columnHeader + " = " + table.size());
		return table.toArray();
	}

	@Override
	public Object[] getRawColumn(String columnHeader) {

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		if(typeRoot == null){ // TODO this null check shouldn't be needed. When we join, we need to add empty nodes--need to call balance at somepoint or something like that
			LOGGER.info("Table is empty............................");
			return new Object[0];
		}
		typeRoot = typeRoot.getLeft(typeRoot);
		List<Object> table = typeRoot.flattenRawToArray(typeRoot, true);
		

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
		LOGGER.info("removing " + columnHeader);	
		LOGGER.info("adujsting names");
		
		String[] newNames = new String[levelNames.length-1];
		int count = 0;
		System.out.println("cur names  " + Arrays.toString(levelNames));
		
//		if(ArrayUtilityMethods.arrayContainsValue(levelNames, columnHeader)) {
//			LOGGER.error("Unable to remove column " + columnHeader + ". Column does not exist in table");
//			return;
//		}
		
		for(String name : levelNames){
			if (count >= newNames.length && (!name.equals(columnHeader))) { // this means a column header was passed in that doesn't exist in the tree
				LOGGER.error("Unable to remove column " + columnHeader + ". Column does not exist in table");
				return;
			}
			if(!name.equals(columnHeader)){
				newNames[count] = name;
				count++;
			}
		}
		this.levelNames = newNames;
		
		this.simpleTree.removeType(columnHeader);
		LOGGER.info("removed " + columnHeader);
		System.out.println("new names  " + Arrays.toString(levelNames));
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
	
	public SimpleTreeBuilder getTree() {
		return simpleTree;
	}
	
	public SimpleTreeBuilder getBuilder(){
		return this.simpleTree;
	}
	
	public static void main(String[] args) {
//		String fileName = "C:\\Users\\bisutton\\Desktop\\BTreeTester.xlsx";
//		String fileName2 = "C:\\Users\\bisutton\\Desktop\\BTreeTester2.xlsx";
//		String fileName3 = "C:\\Users\\bisutton\\Desktop\\BTreeTester3.xlsx";
//		String fileNameout = "C:\\Users\\bisutton\\Desktop\\BTreeOut.xlsx";
		
//		testSerializingAndDeserialing(fileName);
//		testAppend(fileName, fileName2, fileNameout);
//		testStoringAndWriting(fileName, fileNameout);
		//testJoin(fileName, fileName2, fileName3, fileNameout);

		TreeNode newTreeNode = new TreeNode(new StringClass("","",""));
		BTreeIterator it = new BTreeIterator(newTreeNode);
		while(it.hasNext()) {
			it.next();
		}
	}
	
	private static void testSerializingAndDeserialing(String file1){
		BTreeDataFrame tester = load2Tree4Testing(file1);
		
		//TEST SERIALIZING AND DESERIALIZING::::::::::::::::::::::::::::::::::::::::::::::
		TreeNode root = tester.simpleTree.nodeIndexHash.get(tester.levelNames[0]);
		Vector<TreeNode> roots = new Vector<TreeNode>();
		roots.add(root);
		List<SimpleTreeNode> nodes = root.getInstanceNodes(roots, new Vector<SimpleTreeNode>());
		for(SimpleTreeNode node : nodes){
			String serialized = "";
			Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
			vec.add(node);
			serialized = node.serializeTree("", vec, true, 0);
			System.out.println("SERIALIZED " + node.leaf.getKey() + " AS " + serialized);
				
			SimpleTreeNode hookUp = node.deserializeTree(serialized);//
			
			System.out.println("success with  " + hookUp.leaf.getValue());
		}
	}
	
	private static void testAppend(String file1, String file2, String fileOut){
		BTreeDataFrame tester = load2Tree4Testing(file1);
		BTreeDataFrame appender = load2Tree4Testing(file2);
		tester.append(appender);
		
		System.out.println("done fo sho");
		write2Excel4Testing(tester, fileOut);
	}
	
	private static void testJoin(String file1, String file2, String file3, String fileOut){
		BTreeDataFrame tester = load2Tree4Testing(file1);
		String[] names1 = tester.getColumnHeaders();
		
		BTreeDataFrame joiner = load2Tree4Testing(file2);
		String[] names2 = joiner.getColumnHeaders();
		try {
			tester.join(joiner, names1[names1.length-1], names2[0], 1, new ExactStringMatcher());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		names1 = tester.getColumnHeaders();
		System.out.println("done fo sho");
		write2Excel4Testing(tester, fileOut);
		System.out.println("my column...........................................");
		Object[] col = tester.getColumn(names1[names1.length - 1]);
		for(Object row : col){
			System.out.println(row);
		}
		//remove column
//		tester.removeColumn(names1[names1.length - 1]);
//		names1 = tester.getColumnHeaders();
//		
//		BTreeDataFrame joiner3 = load2Tree4Testing(file3);
//		String[] names3 = joiner3.getColumnHeaders();
//		tester.join(joiner3, names1[names1.length-1], names3[0], 1, new ExactStringMatcher());
//
//		names1 = tester.getColumnHeaders();
//		System.out.println("done fo sho");
//		write2Excel4Testing(tester, fileOut);
//		System.out.println("my column...........................................");
//		Object[] col1 = tester.getColumn(names1[names1.length - 1]);
//		for(Object row : col1){
//			System.out.println(row);
//		}
	}
	
	private static void testStoringAndWriting(String file1, String fileOut){
		BTreeDataFrame tester = load2Tree4Testing(file1);
		
		System.out.println("done fo sho");
		write2Excel4Testing(tester, fileOut);
	}
	
	private static BTreeDataFrame load2Tree4Testing(String fileName){
		XSSFWorkbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = new XSSFWorkbook(poiReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		XSSFSheet lSheet = workbook.getSheet("Sheet1");
		
		int lastRow = lSheet.getLastRowNum();
		XSSFRow headerRow = lSheet.getRow(0);
		List<String> headerList = new ArrayList<String>();
		int totalCols = 0;
		while(headerRow.getCell(totalCols)!=null && !headerRow.getCell(totalCols).getStringCellValue().isEmpty()){
			headerList.add(headerRow.getCell(totalCols).getStringCellValue());
			totalCols++;
		}
		Map rowMap = new HashMap();
		BTreeDataFrame tester = new BTreeDataFrame(headerList.toArray(new String[headerList.size()]));
		for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
			XSSFRow row = lSheet.getRow(rIndex);
			for(int cIndex = 0; cIndex<totalCols ; cIndex++)
			{
				String v1 = row.getCell(cIndex).getStringCellValue();
				rowMap.put(headerList.get(cIndex), v1);
			}
			tester.addRow(rowMap, rowMap);
			System.out.println("added row " + rIndex);
			System.out.println(rowMap.toString());
		}
		System.out.println("loaded file " + fileName);
		
		return tester;
	}
	
	private static void write2Excel4Testing(BTreeDataFrame tester, String fileNameout){
		XSSFWorkbook workbookout = new XSSFWorkbook();
		XSSFSheet sheet = workbookout.createSheet("test");
		List<Object[]> data = tester.getData();
//		Iterator<Object[]> it = tester.iterator();
		System.out.println("got flat data. starting to write");
		for(int i = 0; i<data.size(); i++){
//		int i = -1;
//		while(it.hasNext()){
//			i++;
//			Object[] dataR = it.next();
			XSSFRow row = sheet.createRow(i);
			Object[] dataR = data.get(i);
			for(int c = 0; c < dataR.length; c++){
				row.createCell(c).setCellValue(dataR[c] + "");
			}
			System.out.println("wrote row " + i);
		}
		System.out.println("wrote file " + fileNameout);
		
		Utility.writeWorkbook(workbookout, fileNameout);
		testnum++;
	}
}
