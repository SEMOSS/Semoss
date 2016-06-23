/*******************************************************************************

 * Copyright 2015 Defense Health Agency (DHA)
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

package prerna.ds;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openrdf.model.Literal;

import cern.colt.Arrays;
import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class BTreeDataFrame implements ITableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(BTreeDataFrame.class.getName());
	private static final String BIN_COL_NAME_ADDED = "_Bin";
	protected SimpleTreeBuilder simpleTree;
	protected String[] levelNames;
	protected List<String> columnsToSkip;
	protected String[] filteredLevelNames;
	protected Map<String, Boolean> isNumericalMap;
	private Map<String, String> binMap = new HashMap<String, String>();;
	protected Map<String, String> uriMap = new HashMap<String, String>();
	protected List<Object> algorithmOutput = new Vector<Object>();
	
	protected BTreeExcelFilterer filterer;
	
	public BTreeDataFrame(String[] levelNames) {
		setLevelNames(levelNames);
		this.isNumericalMap = new HashMap<String, Boolean>();
		columnsToSkip = new ArrayList<String>();
		filterer = new BTreeExcelFilterer(this);
	}
	
	public BTreeDataFrame(){
		this.isNumericalMap = new HashMap<String, Boolean>();
		columnsToSkip = new ArrayList<String>();
//		filterer = new BTreeExcelFilterer(this);
	}
	
	public BTreeDataFrame(String[] levelNames, String[] uriLevelNames) {
		setLevelNames(levelNames);
		this.isNumericalMap = new HashMap<String, Boolean>();
		for(int i = 0; i < levelNames.length; i++) {
			uriMap.put(levelNames[i], uriLevelNames[i]);
		}
		columnsToSkip = new ArrayList<String>();
		filterer = new BTreeExcelFilterer(this);
	}
	
	protected void setLevelNames(String[] levelNames){
		this.simpleTree = new SimpleTreeBuilder(levelNames[0]);
		this.levelNames = levelNames;
		this.filteredLevelNames = levelNames;
		filterer = new BTreeExcelFilterer(this);
	}
	
	@Override
	public void addRow(ISelectStatement rowData) {
		addRow(rowData.getPropHash(), rowData.getRPropHash());
	}
	
	protected void storeRowInTree(ISEMOSSNode[] row){
		simpleTree.addNodeArray(row);
	}

	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		if(rowCleanData.length != levelNames.length && rowRawData.length != levelNames.length) {
			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe.");
		}
		ISEMOSSNode[] row = new ISEMOSSNode[levelNames.length];
		for(int index = 0; index < levelNames.length; index++) {
			Object val = rowCleanData[index];
			Object rawVal = rowRawData[index];
			//TODO: better way of doing this????
			if(val==null || val.toString().isEmpty()) {
				val = SimpleTreeNode.EMPTY;
			}
			if(rawVal==null || rawVal.toString().isEmpty()) {
				rawVal = SimpleTreeNode.EMPTY;
			}
			row[index] = createNodeObject(val, rawVal, levelNames[index]);

		}
		storeRowInTree(row);
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
			if(val==null || val.toString().isEmpty()) {
				val = SimpleTreeNode.EMPTY;
			}
			if(rawVal==null || rawVal.toString().isEmpty()) {
				rawVal = SimpleTreeNode.EMPTY;
			}
			row[index] = createNodeObject(val, rawVal, levelNames[index]);

		}
		storeRowInTree(row);
	}

	protected ISEMOSSNode createNodeObject(Object value, Object rawValue, String level) {
		ISEMOSSNode node;

		if(value == null) {
			node = new StringClass(null, level); // TODO: fix this
		} else if(value instanceof Integer) {
			node = new IntClass((int)value, (int)value, level);
		} else if(value instanceof Number) {
			node = new DoubleClass(((Number) value).doubleValue(), ((Number) value).doubleValue(), level);
		} else if(value instanceof String) {
			if(rawValue instanceof Literal) {
				node = new StringClass((String)value, (String) value, level);
			} else {
				node = new StringClass((String)value, (String) rawValue.toString(), level);
			}
		}
//		else if(value instanceof Boolean) {
//			node = new BooleanClass((boolean)value, level);
//		}
		else {
			node = new StringClass(value.toString(), level);
		}
		return node;
	}
	
	@Override
	public List<Object[]> getData() {
		List<Object[]> table = new ArrayList<Object[]>();

		Iterator<Object[]> it = this.iterator(false);
		while(it.hasNext()) {
			table.add(it.next());
		}
		return table;
	}
	
	//includes filtered values
	public List<Object[]> getAllData() {
		List<Object[]> table = new ArrayList<Object[]>();
//		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);	
		Iterator<Object[]> it = iteratorAll(false);
		while(it.hasNext()) {
			table.add(it.next());
		}
		return table;
	}
	
	public List<Object[]> getData(String columnHeader, Object value) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode root = this.simpleTree.nodeIndexHash.get(columnHeader);
		TreeNode foundNode = null;
		
		Vector<TreeNode> vec = new Vector<TreeNode>();
		vec.add(root);
		if(value instanceof Number) {
			Double v = ((Number) value).doubleValue();
			TreeNode t = new TreeNode(new DoubleClass(v, columnHeader));
			foundNode = root.getNode(vec, t, false);
		}
		else if(value instanceof String) {
			String v = value.toString();
			TreeNode t = new TreeNode(new StringClass(v, columnHeader));
			foundNode = root.getNode(vec, t, false);
		} else {
			LOGGER.error("value must be either double or string");
		}
		
		UniqueBTreeIterator iterator = new UniqueBTreeIterator(foundNode, false, columnsToSkip);
		int index = ArrayUtilityMethods.arrayContainsValueAtIndex(filteredLevelNames, columnHeader);
		while(iterator.hasNext()) {
		List<Object[]> returnData = iterator.next();
		Object[] returnRow = returnData.get(0);
			if(returnRow[index].toString().equalsIgnoreCase(value.toString())) {
				return returnData;
			}
		}
		
//		return (foundNode==null) ? null: new UniqueBTreeIterator(foundNode, false, columnsToSkip).next();
		return null;
	} 
	
	public List<Object[]> getScaledData() {
		List<Object[]> retData = new ArrayList<Object[]>();
		Iterator<Object[]> iterator = this.scaledIterator(false);
		while(iterator.hasNext()) {
			retData.add(iterator.next());
		}
		return retData;
	}
	
	@Override
	public List<Object[]> getScaledData(List<String> exceptionColumns) {
		List<Object[]> retData = new ArrayList<Object[]>();
		boolean[] exceptCol = new boolean[levelNames.length];
		for(int i = 0; i < exceptCol.length; i++) {
			if(exceptionColumns.contains(levelNames)) {
				exceptCol[i] = true;
			} else {
				exceptCol[i] = false;
			}
		}
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);
		Iterator<Object[]> iterator = new ScaledBTreeIterator(typeRoot, this.isNumeric(), this.getMin(), this.getMax(), false, columnsToSkip, exceptCol);
		while(iterator.hasNext()) {
			retData.add(iterator.next());
		}
		return retData;
	}
	
	@Override
	public List<Object[]> getRawData() {
		List<Object[]> table = new ArrayList<Object[]>();

		Iterator<Object[]> it = this.iterator(true);
		while(it.hasNext()) {
			table.add(it.next());
		}

		return table;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
		LOGGER.info("Columns Passed ::: " + colNameInTable + " and " + colNameInJoiningTable);
		LOGGER.info("Confidence Threshold :: " + confidenceThreshold);
		LOGGER.info("Analytics Routine ::: " + routine.getName());
		LOGGER.info("Begining join on columns ::: " + colNameInTable + " and " + colNameInJoiningTable);

		if(this.isEmpty()) {
			LOGGER.info("this table is empty, join halted");
			return;
		} else if(table.isEmpty()) {
			LOGGER.info("Table Argument is empty, could not join");
			return;
		}
		//TODO: improve logic.. add error handling
//		if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(this.levelNames, colNameInTable)) {
//			colNameInTable = colNameInTable.toUpperCase();
//		}
//		if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(table.getColumnHeaders(), colNameInJoiningTable)) {
//			colNameInJoiningTable = colNameInJoiningTable.toUpperCase();
//		}
		colNameInTable = this.getColumnName(this.levelNames, colNameInTable);
		colNameInJoiningTable = this.getColumnName(table.getColumnHeaders(), colNameInJoiningTable);
		
		// fill the options needed for the routine
		List<SEMOSSParam> params = routine.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(params.get(0).getName(), colNameInTable);
		selectedOptions.put(params.get(1).getName(), colNameInJoiningTable);
		//if the routine takes in confidence threshold, must pass that in as well
		if(params.size()>2) {
			selectedOptions.put(params.get(2).getName(), confidenceThreshold);
		}
		routine.setSelectedOptions(selectedOptions);

		// let the routine run
		LOGGER.info("Begining matching routine");
		System.err.println("getting matches");
		long startTime = System.currentTimeMillis();
		List<TreeNode[]> matched = routine.runAlgorithm(this, table);
		System.err.println("got matches"+(System.currentTimeMillis() - startTime)+" ms");
		
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
//			String[] uriColumnNames = passedTree.getURIColumnHeaders();
			// add the new data to this tree
			LOGGER.info("Augmenting tree");
			int origLength = this.levelNames.length;	
//			joinTreeLevels(columnNames, uriColumnNames, colNameInJoiningTable); // need to add new levels to this tree's level array
//			List<Object[]> flatMatched = matched.getData();// TODO: this could be replaced with nextRow or getRow method directly on the tree

			TreeNode thisRootNode = this.simpleTree.nodeIndexHash.get(colNameInTable); //TODO: is there a better way to get the type? I don't think this is reliable
			//this.simpleTree.adjustType(colNameInTable, true);

			Vector thisSearchVector = new Vector();
			thisSearchVector.addElement(thisRootNode);

			SimpleTreeBuilder passedBuilder = passedTree.getBuilder();
			TreeNode passedRootNode = passedBuilder.nodeIndexHash.get(colNameInJoiningTable); //TODO: is there a better way to get the type? I don't think this is reliable
			Vector passedSearchVector = new Vector();
			passedSearchVector.addElement(passedRootNode);

			boolean innerJoin = true;
			// store the last empty node in a trail of empty nodes starting from root for outer joins
			SimpleTreeNode lastEmptyNodeInTrailOfEmptyTreeNode = null;
			// store a list of empty nodes for partial joins
			SimpleTreeNode startEmptyNodeForNewTree = null;
			
			// Iterate for every row in the matched table
			for(TreeNode[] flatMatchedRow : matched) { // for each matched item
				TreeNode thisTreeNode = flatMatchedRow[0];
				TreeNode passedTreeNode = flatMatchedRow[1];
//				System.out.println(item1 + "           " + item2);
				
				// search for tree node in this table and tree node in passed table
//				TreeNode thisSearchNode = new TreeNode(createNodeObject(item1, item1, colNameInTable)); //TODO: how do we generically do this...?
//				TreeNode thisTreeNode = thisRootNode.getNode(thisSearchVector, thisSearchNode, false);
//				
//				TreeNode passedSearchNode = new TreeNode(createNodeObject(item2, item2, colNameInJoiningTable)); //TODO: how do we generically do this...?
//				TreeNode passedTreeNode = passedRootNode.getNode(passedSearchVector, passedSearchNode, false);
				
				// these are the instances that will be be joined to in the existing tree
				// this will be null when doing an outer join
				
				Object thisVal = thisTreeNode.leaf.getValue();
				Object passedVal = passedTreeNode.leaf.getValue();
				
				// only return empty value for passVal in outer joins
				// this will be cleaned up when we adjust type - set innerJoin to false
				if(passedVal.equals(SimpleTreeNode.EMPTY)) {
					innerJoin = false;
					continue;
				}
				
				Vector <SimpleTreeNode> thisInstances = null;
				thisInstances = thisTreeNode.getInstances();
					
				Vector <SimpleTreeNode> passedInstances = null;
				passedInstances = passedTreeNode.getInstances();

				SimpleTreeNode instance2HookUp = null;
				
				// these series of if statements is to properly determine what the instance2HookUp and the passedInstances objects are
				// they are determined based on the values retrieved in the flatMatched list
				if(thisVal.equals(SimpleTreeNode.EMPTY) && !passedVal.equals(SimpleTreeNode.EMPTY)) {
					/*
					 * Logic is to create (or find) a trail of empty nodes from the root and then connect to the values of item2
					 */
					innerJoin = false;
					instance2HookUp = passedInstances.get(0).leftChild;
					// get the last empty tree node and set it in thisInstances list
					if(lastEmptyNodeInTrailOfEmptyTreeNode == null) {
						// create and store the last node in the trail of empty nodes
						lastEmptyNodeInTrailOfEmptyTreeNode = this.simpleTree.createTrailOfEmptyNodes(levelNames, origLength);
					}
					thisInstances = new Vector <SimpleTreeNode>();
					thisInstances.add(lastEmptyNodeInTrailOfEmptyTreeNode);

//				} 
//				else if(!item1.equals(SimpleTreeNode.EMPTY) && item2.equals(SimpleTreeNode.EMPTY)) {
//					innerJoin = false;
//					/*
//					 * Logic is for the item1 node, to add a trail of empty nodes after it equal to the length of what would be added
//					 */
//					
//					// we create a set of empty nodes
//					// currently assume we are always joining to the tree passed in starting from the root
//					// that is why the start index is 1 and goes until the length of the number of headers
//					if(startEmptyNodeForNewTree == null) {
//						startEmptyNodeForNewTree = getSetOfEmptyNodes(columnNames, 1, columnNames.length);
//					}
//					instance2HookUp = startEmptyNodeForNewTree;
					
				} else {
					instance2HookUp = passedInstances.get(0).leftChild;
					//TODO: need to make generic logic
					//TODO: assuming that we are only joining right to left, left child will never be null
					//TODO: need to revisit logic for when joining columns both to the left and right
					if(instance2HookUp == null) {
						//TODO: do not break, throw an exception
						break;
					}
				}
				
				// this is where the actual hooking up occurs
//				Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
//				vec.add(instance2HookUp);
//				String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
				String serialized = SimpleTreeNode.serializeTree(instance2HookUp);

				// hook up passed tree node with each instance of this tree node
				for(int instIdx = 0; instIdx < thisInstances.size(); instIdx++){
					SimpleTreeNode myNode = thisInstances.get(instIdx);
					SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
					SimpleTreeNode.addLeafChild(myNode, hookUp);
				}
			}
			
			//Update the Index Tree
			System.err.println("updating index tree in join");
			startTime = System.currentTimeMillis();
			TreeNode treeRoot = this.simpleTree.nodeIndexHash.get(levelNames[origLength-1]);
			ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(treeRoot);
			while(iterator.hasNext()) {
				SimpleTreeNode t = iterator.next();
				this.simpleTree.appendToIndexTree(t.leftChild);
			}
			System.err.println("updated index tree in join: "+(System.currentTimeMillis() - startTime)+" ms");
			
			System.err.println("trimming tree for inner join");
			startTime = System.currentTimeMillis();
			if(innerJoin) {
				this.simpleTree.removeBranchesWithoutMaxTreeHeight(levelNames[0], levelNames.length);
				this.simpleTree.deleteFilteredValues(this.simpleTree.getRoot());
				for (String level : levelNames) {
					this.simpleTree.nodeIndexHash.put(level, this.simpleTree.refreshIndexTree(level));
				}
			} else {
				this.simpleTree.adjustType(levelNames[origLength-1], true);
			}
			System.err.println("trimmed tree: "+(System.currentTimeMillis() - startTime)+" ms");
		}
		else 
		{
			LOGGER.error("Cannot join something not a btree with a btree");
		}
		this.isNumericalMap.remove(colNameInTable);
	}

//	/**
//	 * This method will return a series of simple tree nodes with empty values connected to each other
//	 * It returns the root of the series
//	 * @param columnNames				String[] containing the list of types for each simple tree node
//	 * @param startIndex				The start index of the simple tree node series, inclusive
//	 * @param endIndex					The end index of the simple tree node series, exclusive
//	 * @return							The root of the series of simple tree nodes
//	 */
//	private SimpleTreeNode getSetOfEmptyNodes(String[] columnNames, int startIndex, int endIndex) {
//		// create empty nod
//		ITreeKeyEvaluatable emptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, columnNames[startIndex]);
//		SimpleTreeNode startNode = new SimpleTreeNode(emptyVal);
//		SimpleTreeNode dummy = startNode;
//		int i = startIndex;
//		// starting at start index, loop through and add empty nodes until desired length
//		while(i < endIndex) {
//			ITreeKeyEvaluatable newEmptyVal = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, columnNames[i+1]);
//			SimpleTreeNode newEmpty = new SimpleTreeNode(newEmptyVal);
//			dummy.leftChild = newEmpty;
//			newEmpty.parent = dummy;
//			dummy = newEmpty;
//			i++;
//		}
//		
//		// return the start node
//		return startNode;
//	}
	
	//TODO: need to ensure there are not two names that match 
    private String[] joinTreeLevels(String[] joinLevelNames, String[] uriJoinLevelNames, String colNameInJoiningTable) {
        String[] newLevelNames = new String[this.levelNames.length + joinLevelNames.length - 1];
        // copy old values to new
        System.arraycopy(levelNames, 0, newLevelNames, 0, levelNames.length);
        int newNameIdx = 0;
        String[] onlyNewNames = new String[joinLevelNames.length - 1];
        for(int i = 0; i < joinLevelNames.length; i++) {
           String name = joinLevelNames[i];
           if(!name.equalsIgnoreCase(colNameInJoiningTable)) {
             newLevelNames[newNameIdx + levelNames.length] = joinLevelNames[i];
             onlyNewNames[newNameIdx] = joinLevelNames[i];
             uriMap.put(joinLevelNames[i], uriJoinLevelNames[i]);
             newNameIdx ++;
           }
        }

        this.levelNames = newLevelNames;
        this.adjustFilteredColumns();
        return onlyNewNames;
    }
    
    private String[] joinTreeLevels(String[] joinLevelNames, String[] uriJoinLevelNames, String[] colNamesInJoiningTable) {
    	String[] newLevelNames = new String[this.levelNames.length + joinLevelNames.length - colNamesInJoiningTable.length];
        // copy old values to new
        System.arraycopy(levelNames, 0, newLevelNames, 0, levelNames.length);
        int newNameIdx = 0;
        String[] onlyNewNames = new String[joinLevelNames.length - colNamesInJoiningTable.length];
        for(int i = 0; i < joinLevelNames.length; i++) {
           String name = joinLevelNames[i];
           if(!ArrayUtilityMethods.arrayContainsValue(colNamesInJoiningTable, name)) {
             newLevelNames[newNameIdx + levelNames.length] = joinLevelNames[i];
             onlyNewNames[newNameIdx] = joinLevelNames[i];
             uriMap.put(joinLevelNames[i], uriJoinLevelNames[i]);
             newNameIdx ++;
           }
        }

        this.levelNames = newLevelNames;
        this.adjustFilteredColumns();
        return onlyNewNames;
    }


    public void join(ITableDataFrame table, String[] colHeadersInTable, String[] colHeadersInJoiningTable, double confidenceThreshold) {
    	int origLength = this.levelNames.length;

//    	this.joinTreeLevels(table.getColumnHeaders(), table.getURIColumnHeaders(), colHeadersInJoiningTable);
    	BTreeDataFrameJoiner.join(this, (BTreeDataFrame)table, colHeadersInTable, colHeadersInJoiningTable);
    	
    	this.simpleTree.removeBranchesWithoutMaxTreeHeight(levelNames[0], levelNames.length);
		
    	
    	
    	TreeNode treeRoot = this.simpleTree.nodeIndexHash.get(levelNames[origLength-1]);
		ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(treeRoot);
		while(iterator.hasNext()) {
			SimpleTreeNode t = iterator.next();
			this.simpleTree.appendToIndexTree(t.leftChild);
			//this.simpleTree.appendToFilteredIndexTree(t.rightChild);
		}
		
		for(String column : colHeadersInTable) {
			this.isNumericalMap.remove(column);
		}
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
				simpleTree.append(this.simpleTree.getRoot(), bTree.getBuilder().getRoot()); break;
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
	public void performAnalyticTransformation(IAnalyticTransformationRoutine routine) {
		ITableDataFrame newTable = routine.runAlgorithm(this);
		this.join(newTable, newTable.getColumnHeaders()[0], newTable.getColumnHeaders()[0], 1, new ExactStringMatcher());
	}
	
	@Override
	public void performAnalyticAction(IAnalyticActionRoutine routine) {
		routine.runAlgorithm(this);
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		int count = 0;
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			it.next();
			count++;
		}
		return count;
	}

	@Override
	public Integer[] getUniqueInstanceCount() {
		Integer[] counts = new Integer[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			counts[i] = getUniqueInstanceCount(filteredLevelNames[i]);
		}
		return counts;
	}
	
	public Object[] getUniqueValues(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		List<Object> uniqueValues = new ArrayList<Object>();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		Iterator<Object> it = new UniqueValueIterator(typeRoot, false, false);
		while(it.hasNext()) {
			uniqueValues.add(it.next());
		}
		
		return uniqueValues.toArray();
	}
	
	@Override
	public Object[] getUniqueRawValues(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		List<Object> uniqueValues = new ArrayList<Object>();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			uniqueValues.add(it.next().leaf.getRawValue());
		}
		
		return uniqueValues.toArray();
	}

	public Object[] getFilteredUniqueRawValues(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		List<Object> uniqueValues = new ArrayList<Object>();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		IndexTreeIterator it = new IndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			TreeNode t = it.next();
			if(t.getHardFilteredInstances().size() > 0) {
				uniqueValues.add(t.leaf.getRawValue());
			}
		}
		
		return uniqueValues.toArray();
	}
	
	@Override
	public Map<String, Integer> getUniqueValuesAndCount(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		Map<String, Integer> valueCount = new HashMap<String, Integer>();
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			TreeNode node = it.next();
			valueCount.put(node.leaf.getValue().toString(), node.getInstances().size());
		}
		return valueCount;
		
//		Object[] countColumn = this.getColumn(columnHeader); //get all the objects within the column
//		Map<String, Integer> returnHash = new HashMap<>(); //initiate new HashMap
//		
//		int count;
//	
//		for (int i = 0; i < countColumn.length; i++) {    //loop until column ends
//			String currentKey = countColumn[i].toString();
//			if (returnHash.containsKey(currentKey)) {    //if the specific value in the object is already in a HashMap:
//				count = returnHash.get(currentKey)+1;
//				returnHash.put(currentKey, count);    //add to HashMap
//			} else {    //if specific value isn't already in a HashMap:
//				returnHash.put(currentKey, 1);    //add to HashMap
//			}
//		}
//
//		return returnHash;
	}

	@Override
	public Map<String, Map<String, Integer>> getUniqueColumnValuesAndCount() {
		Map<String, Map<String, Integer>> uniqueColsAndValueCount = new HashMap<String, Map<String, Integer>>();
		for(String colName : filteredLevelNames) {
			Map<String, Integer> uniqueValuesAndCount = getUniqueValuesAndCount(colName);
			uniqueColsAndValueCount.put(colName, uniqueValuesAndCount);
		}
		return uniqueColsAndValueCount;
	}
	
	public Double getEntropy(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		double entropy = 0;
		if(isNumeric(columnHeader)) {
			//TODO: need to make barchart class better
			Double[] dataRow = (Double[]) getColumn(columnHeader);
			int numRows = dataRow.length;
			Hashtable<String, Object>[] bins = null;
			BarChart chart = new BarChart(dataRow);
			
			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("NaN", true, true);
				chart.generateJSONHashtableCategorical();
				bins = chart.getRetHashForJSON();
			} else {
				chart.generateJSONHashtableNumerical();
				bins = chart.getRetHashForJSON();
			}
			
			int i = 0;
			int uniqueValues = bins.length;
			for(; i < uniqueValues; i++) {
				int count = (int) bins[i].get("y");
				if(count != 0) {
					double prob = (double) count / numRows;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			
		} else {
			Map<String, Integer> uniqueValuesAndCount = getUniqueValuesAndCount(columnHeader);
			Integer[] counts = (Integer[]) uniqueValuesAndCount.values().toArray();
			
			// if only one value, then entropy is 0
			if(counts.length == 1) {
				return entropy;
			}
			
			double sum = StatisticsUtilityMethods.getSum(counts);
			int index;
			for(index = 0; index < counts.length; index++) {
				double val = counts[index];
				if(val != 0) {
					double prob = val / sum;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}		
		}
		
		return -1.0 *entropy;
	}

	public Double[] getEntropy() {
		Double[] entropyValues = new Double[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			entropyValues[i] = getEntropy(filteredLevelNames[i]);
		}
		return entropyValues;
	}
	
	@Override
	public Double getEntropyDensity(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		double entropyDensity = 0;
		
		if(isNumeric(columnHeader)) {
			//TODO: need to make barchart class better
			Double[] dataRow = getColumnAsNumeric(columnHeader);
			int numRows = dataRow.length;
			Hashtable<String, Object>[] bins = null;
			BarChart chart = new BarChart(dataRow);
			
			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("NaN", true, true);
				chart.generateJSONHashtableCategorical();
				bins = chart.getRetHashForJSON();
			} else {
				chart.generateJSONHashtableNumerical();
				bins = chart.getRetHashForJSON();
			}
			
			double entropy = 0;
			int i = 0;
			int uniqueValues = bins.length;
			for(; i < uniqueValues; i++) {
				int count = (int) bins[i].get("y");
				if(count != 0) {
					double prob = (double) count / numRows;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = (double) entropy / uniqueValues;
			
		} else {
			Map<String, Integer> uniqueValuesAndCount = getUniqueValuesAndCount(columnHeader);
			Integer[] counts = uniqueValuesAndCount.values().toArray(new Integer[]{});
			
			// if only one value, then entropy is 0
			if(counts.length == 1) {
				return entropyDensity;
			}
			
			double entropy = 0;
			double sum = StatisticsUtilityMethods.getSum(counts);
			int index;
			for(index = 0; index < counts.length; index++) {
				double val = counts[index];
				if(val != 0) {
					double prob = val / sum;
					entropy += prob * StatisticsUtilityMethods.logBase2(prob);
				}
			}
			entropyDensity = entropy / uniqueValuesAndCount.keySet().size();
		}
		
		return -1.0 * entropyDensity;
	}

	public Double[] getEntropyDensity() {
		Double[] entropyDensityValues = new Double[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			entropyDensityValues[i] = getEntropyDensity(filteredLevelNames[i]);
		}
		return entropyDensityValues;
	}

	@Override
	public Double getMax(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		if(!isNumeric(columnHeader)) {
			return Double.NaN;
		}
		
		//first value returned by iterator is the 'least' value
		TreeNode root = this.simpleTree.nodeIndexHash.get(columnHeader);
		Iterator<TreeNode> iterator = new ReverseIndexTreeIterator(root);
		if(iterator.hasNext()) {
			TreeNode node = iterator.next();
			if(node.leaf.getValue() instanceof Number) {
				return ((Number) node.leaf.getValue()).doubleValue();
			}
		}
		
		return Double.NaN;
	}

	@Override
	public Double[] getMax() {
		Double[] maxValues = new Double[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			maxValues[i] = getMax(filteredLevelNames[i]);
		}
		return maxValues;
	}

	@Override
	public Double getMin(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		if(!isNumeric(columnHeader)) {
			return Double.NaN;
		}
		
		//first value returned by iterator is the 'least' value
		Iterator<Object> iterator = this.uniqueValueIterator(columnHeader, false, false);
		if(iterator.hasNext()) {
			Object value = this.uniqueValueIterator(columnHeader, false, false).next();
			if(value instanceof Number) {
				return ((Number) value).doubleValue();
			}
		}
		
		return Double.NaN;
//		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
//		while(typeRoot.leftChild != null) {
//			typeRoot = typeRoot.leftChild;
//		}
//		
//		if(typeRoot.leaf.isEqual(new StringClass(SimpleTreeNode.EMPTY))) {
//			if(typeRoot.rightSibling != null) {
//				typeRoot = typeRoot.rightSibling;
//			} else {
//				typeRoot = typeRoot.parent;
//			}
//		}
//		return ((Number) typeRoot.leaf.getValue()).doubleValue();
	}

	@Override
	public Double[] getMin() {
		Double[] minValues = new Double[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			minValues[i] = getMin(filteredLevelNames[i]);
		}
		return minValues;
	}

	public Double getAverage(String columnHeader) {
//		columnHeader = this.getColumnName(columnHeader);
//
//		double sum = 0;
//		double count = 0;
//
//		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
//		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
//		StringClass empty = new StringClass(SimpleTreeNode.EMPTY);
//		while(it.hasNext()) {
//			TreeNode node = it.next();
//			ITreeKeyEvaluatable val = node.leaf;
//			if(val instanceof StringClass) {
//				if(val.isEqual(empty)) {
//					continue;
//				} else {
//					return Double.NaN;
//				}
//			}
//			sum += ((Number) node.leaf.getValue()).doubleValue() * node.getInstances().size();
//			count++;
//		}
//
//		return sum / count;
		
		double sum = this.getSum(columnHeader); //sum column
		double count = this.getNumRows(); //count length of column
		
		return sum / count;

	}

	public Double[] getAverage() {
		Double[] averageValues = new Double[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			averageValues[i] = getAverage(filteredLevelNames[i]);
		}
		return averageValues;
	}

	public Double getSum(String columnHeader) {
//		columnHeader = this.getColumnName(columnHeader);
//
//		double sum = 0;
//		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
//		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
//		StringClass empty = new StringClass(SimpleTreeNode.EMPTY);
//		while(it.hasNext()) {
//			TreeNode node = it.next();
//			ITreeKeyEvaluatable val = node.leaf;
//			if(val instanceof StringClass) {
//				if(val.isEqual(empty)) {
//					continue;
//				} else {
//					return Double.NaN;
//				}
//			}
//			sum += ((Number) val.getValue()).doubleValue() * node.getInstances().size();
//		}
//		
//		return sum;
		
		columnHeader = this.getColumnName(columnHeader);  //get column name
		Object[] getColumn = this.getColumn(columnHeader);  //get all objects within the colum
		double sum = 0;  

		//for the length of the column sum all number values
		for (int i = 0; i < getColumn.length; i++) { 
			//if object isn't a number: continue if blank or return not a number
			if (getColumn[i] instanceof StringClass) {  
				if (getColumn[i] == null) {
					continue;
				} else {
					return Double.NaN;
				}
			} else {
				double addValue = ((Number) getColumn[i]).doubleValue();
				sum += addValue;
			}
		}
		return sum;

	}

	@Override
	public boolean isNumeric(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		if(isNumericalMap.containsKey(columnHeader)) {
			Boolean isNum = isNumericalMap.get(columnHeader);
			if(isNum != null) {
				return isNum;
			}
		} else {
			isNumericalMap.put(columnHeader, null);
		}
		
		
		boolean isNumeric = false;
		
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		IndexTreeIterator it = new IndexTreeIterator(typeRoot);
		StringClass empty = new StringClass(SimpleTreeNode.EMPTY);
		while(it.hasNext()) {
			ITreeKeyEvaluatable val = it.next().leaf;
			if(val instanceof StringClass) {
				if(val.isEqual(empty)) {
					continue;
				} else {
					isNumeric = false;
					isNumericalMap.put(columnHeader, isNumeric);
					return isNumeric;
				}
			}
			isNumeric = true;
		}
		
		isNumericalMap.put(columnHeader, isNumeric);
		return true;
	}

	@Override
	public boolean[] isNumeric() {
		boolean[] isNumeric = new boolean[filteredLevelNames.length];
		for(int i = 0; i < filteredLevelNames.length; i++) {
			isNumeric[i] = isNumeric(filteredLevelNames[i]);
		}
		return isNumeric;
	}

	@Override
	public String[] getColumnHeaders() {
		return this.filteredLevelNames;
	}

	@Override
	public int getNumRows() {
		int numRows = 0;
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);
		FilteredIndexTreeIterator it = new FilteredIndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			numRows += it.next().getInstances().size();
		}
		return numRows;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);	
		return new BTreeIterator(typeRoot, getRawData, columnsToSkip);
	}
	
	public Iterator<Object[]> iteratorAll(boolean getRawData){
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);	
		return new BTreeIterator(typeRoot, getRawData, columnsToSkip, true);
	}
	
	@Override
	public Iterator<Object[]> scaledIterator(boolean getRawData) {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);
		return new ScaledBTreeIterator(typeRoot, this.isNumeric(), this.getMin(), this.getMax(), getRawData, columnsToSkip);
	}
	
//	public Iterator<Object[]> scaledIterator(boolean getRawData, List<String> exceptionColumns) {
//		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length-1]);
//		int[] exceptionIndex = new int[exceptionColumns.size()];		
//		return new ScaledBTreeIterator(typeRoot, this.isNumeric(), this.getMin(), this.getMax(), getRawData, columnsToSkip, ExceptionIndex);
//	}

	@Override
	public Iterator<List<Object[]>> uniqueIterator(String columnHeader, boolean getRawData) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);	
		return new UniqueBTreeIterator(typeRoot, getRawData, columnsToSkip);
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData, Map<String, Object> options) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		return new ScaledUniqueBTreeIterator(typeRoot, this.isNumeric(), this.getMin(), this.getMax(), getRawData, columnsToSkip);
	}
	
	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		return new UniqueValueIterator(typeRoot, getRawData, iterateAll);
	}
	
	@Override
	public Object[] getColumn(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);


		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[levelNames.length - 1]);
		if(typeRoot == null){ // TODO this null check shouldn't be needed. When we join, we need to add empty nodes--need to call balance at somepoint or something like that
			LOGGER.info("Table is empty............................");
			return new Object[0];
		}
		
		//Need to make this more efficient
		int index = ArrayUtilityMethods.arrayContainsValueAtIndex(levelNames, columnHeader);
		Iterator<Object[]> iterator = new BTreeIterator(typeRoot, false, null);
		List<Object> column = new ArrayList<Object>();
		while(iterator.hasNext()) {
			column.add(iterator.next()[index]);
		}
		
		return column.toArray();
	}
	
	public String[] getColumnAsString(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		ValueTreeColumnIterator it = new ValueTreeColumnIterator(typeRoot);
		List<String> retList = new ArrayList<String>();
		while(it.hasNext()) {
			String value = it.next().leaf.getValue().toString();
			retList.add(value);
		}
		return retList.toArray(new String[0]);
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		if(!isNumeric(columnHeader)) {
			return null;
		}
		
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		ValueTreeColumnIterator it = new ValueTreeColumnIterator(typeRoot);
		List<Double> retList = new ArrayList<Double>();
		while(it.hasNext()) {
			Object value = it.next().leaf.getValue();
			if(value instanceof String) {
				retList.add(null);
			}
			else {
				retList.add( ((Number) value).doubleValue());
			}
		}
		return retList.toArray(new Double[0]);
	}
	
	@Override
	public Object[] getRawColumn(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
		if(typeRoot == null){ 
			LOGGER.info("Table is empty............................");
			return new Object[0];
		}
		typeRoot = TreeNode.getLeft(typeRoot);
		List<Object> table = typeRoot.flattenRawToArray(typeRoot, true);
		

		System.out.println("Final count for column " + columnHeader + " = " + table.size());
		return table.toArray();
	}
	
	@Override
	public void removeColumn(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);
		
		String[] newNames = new String[levelNames.length-1];
		int count = 0;
		
		for(int i = 0; i < levelNames.length; i++) {
			String name = levelNames[i];
			if (count >= newNames.length && (!name.equals(columnHeader))) { // this means a column header was passed in that doesn't exist in the tree
				LOGGER.error("Unable to remove column " + columnHeader + ". Column does not exist in table");
				return;
			}
			if(!name.equals(columnHeader)) {
				newNames[count] = name;
				count++;
			} 
//			else {
//				colToRemoveName = levelNames[i];
//			}
		}
		this.levelNames = newNames;
		this.adjustFilteredColumns();
		this.simpleTree.removeType(columnHeader);
		isNumericalMap.remove(columnHeader);
		this.simpleTree.setRootLevel(levelNames[0]);
//		LOGGER.info("removed " + columnHeader);
//		System.out.println("new names  " + Arrays.toString(levelNames));
	}
	
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		columnHeader = this.getColumnName(columnHeader);
		
//		IndexTreeIterator iterator = new IndexTreeIterator(this.getBuilder().nodeIndexHash.get(columnHeader));
//		while(iterator.hasNext()) {
//			TreeNode node = iterator.next();
//			if(node.instanceNode.size() == 0) {
//				filterValues.add(node.getValue());
//			}
//		}
//		for(Object o: filterValues) {
//			this.filterer.filterTree(this.createNodeObject(o, o, columnHeader));
//		}
		
		this.filterer.filter(columnHeader, filterValues);
		this.isNumericalMap.clear();
	}
	
	public void unfilter() {
		for(String column: levelNames) {
			this.unfilter(column);
		}
	}
	
	public Object[] getFilterModel() {
		return filterer.getRawFilterModel();
	}
	
	public Map<String, Object[]> getFilterTransformationValues() {
		return filterer.getFilterTransformationsValues();//getFilterTransformationValues();
	}
	
	//remove M's from the filterValue nodes, decrement all their sub tree values' TM count
	public void unfilter(String columnHeader, List<Object> filterValues) {
		columnHeader = this.getColumnName(columnHeader);
		
		for(Object o: filterValues) {
			this.filterer.unfilterTree(this.createNodeObject(o, o, columnHeader));
		}

	}
	
	@Override
	//remove all M's from this column
	public void unfilter(String columnHeader) {
		columnHeader = this.getColumnName(columnHeader);
		
//		IndexTreeIterator iterator = new IndexTreeIterator(this.simpleTree.nodeIndexHash.get(columnHeader));
//		while(iterator.hasNext()) {
//			TreeNode t = iterator.next();
//			Object o = t.leaf.getValue();
//			this.filterer.unfilterTree(this.createNodeObject(o, o, columnHeader));
//		}
		this.filterer.unfilter(columnHeader);
	}

	//TODO: is this necessary
	public String[] getTreeLevels() {
		return this.filteredLevelNames;
	}
	
	protected SimpleTreeBuilder getBuilder(){
		return this.simpleTree;
	}
	
	@Override
	public boolean isEmpty() {
		return !this.iterator(false).hasNext();
	}
	
	@Override
	public void setColumnsToSkip(List<String> columnHeaders) {
		List<String> newColumnHeaders = new ArrayList<String>();
		if(columnHeaders != null) {
			for(String col : columnHeaders) {
				newColumnHeaders.add(getColumnName(col));
			}
		}
		columnsToSkip = newColumnHeaders;
		adjustFilteredColumns();
	}
	
	public void filterColumns(String[] columnHeaders) {
		for(String column : columnHeaders) {
			columnsToSkip.add(column);
		}
		adjustFilteredColumns();
	}
	
	public void unfilterColumns(String[] columnHeaders) {
		for(String column : columnHeaders) {
			columnsToSkip.remove(column);
		}
		adjustFilteredColumns();
	}
	
	public String[] getFilteredColumns() {
		return columnsToSkip.toArray(new String[columnsToSkip.size()]);
	}
	
	
	private void adjustFilteredColumns() {
		
		ArrayList<String> fc = new ArrayList<String>();
		if(columnsToSkip == null) {
			this.filteredLevelNames = this.levelNames;
		} else if(columnsToSkip.size() == 0) {
			this.filteredLevelNames = this.levelNames;
		} else {
			for(String level: levelNames) {
				if(!columnsToSkip.contains(level)) {
					fc.add(level);
				}
			}
			this.filteredLevelNames = fc.toArray(new String[fc.size()]);
		}
	}
	
	private String getColumnName(String columnHeader) {		
		for(String level : levelNames) {
			if(level.equalsIgnoreCase(columnHeader)) {
				return level;
			}
		}
		//TODO: this is an annoying hack since the query parser returns do not match
		//		the owl concepts since variable names cannot contain some characters
		//		that URIs concept types can
		//		Hack is also in other overloaded getColumnName method
		String cleanColHeader = Utility.cleanVariableString(columnHeader);
		for(String level : levelNames) {
			if(level.equalsIgnoreCase(cleanColHeader)) {
				return level;
			}
		}
		throw new IllegalArgumentException("Could not find match for "+columnHeader+" in level names: "+ Arrays.toString(levelNames));
	}
	
	private String getColumnName(String[] columnHeaders, String columnHeader) {		
		for(String level : columnHeaders) {
			if(level.equalsIgnoreCase(columnHeader)) {
				return level;
			}
		}
		//TODO: this is an annoying hack since the query parser returns do not match
		//		the owl concepts since variable names cannot contain some characters
		//		that URIs concept types can
		//		Hack is also in other overloaded getColumnName method
		String cleanColHeader = Utility.cleanVariableString(columnHeader);
		for(String level : columnHeaders) {
			if(level.equalsIgnoreCase(cleanColHeader)) {
				return level;
			}
		}
		throw new IllegalArgumentException("Could not find match for "+columnHeader+" in level names: "+ Arrays.toString(levelNames));
	}

	
	public static void main(String[] args) {


//		List<Object> capFilter = new ArrayList<Object>();
//		List<Object> lowerFilter = new ArrayList<Object>();
//		List<Object> numFilter = new ArrayList<Object>();
		
//		capFilter.add("A2");
//		capFilter.add("A1025");
//		capFilter.add("A1000");
//		lowerFilter.add("a3");
//		lowerFilter.add("a1998");
//		numFilter.add("1");
//		numFilter.add("1999");
//		
//		newTree.filter("Capital", capFilter);
//		newTree.filter("Lowercase", lowerFilter);
//		newTree.filter("Number", numFilter);
//		System.out.println("Values filtered");
//		
//		newTree.refresh();
//		SimpleTreeNode root = newTree.simpleTree.getRoot();
//		write2Excel4Testing(tree, fileOut);
		
		// System.out.println("****OLD TREE****");
		// Iterator<Object> oldCapIterator = newTree.uniqueValueIterator("Capital", false, true); // Change for each query
		// while (oldCapIterator.hasNext()) {
		// System.out.println(oldCapIterator.next().toString());
		// }
		// Iterator<Object> oldLowerIterator = newTree.uniqueValueIterator("Lowercase", false, true); // Change for each query
		// while (oldLowerIterator.hasNext()) {
		// System.out.println(oldLowerIterator.next().toString());
		// }
		// Iterator<Object> oldNumIterator = newTree.uniqueValueIterator("Number", false, true); // Change for each query
		// while (oldNumIterator.hasNext()) {
		// System.out.println(oldNumIterator.next().toString());
		// }

		// // String serial = TreeNode.serializeTree(newTree.simpleTree.nodeIndexHash.get("Capital"));
		// // TreeNode newNode = TreeNode.deserializeTree(serial);
		// // CompleteIndexTreeIterator it = new CompleteIndexTreeIterator(newNode);
		// FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
		// double start = System.currentTimeMillis();
		// TreeNode root = newTree.simpleTree.nodeIndexHash.get("Capital");
		// TreeNode node = conf.deepCopy(root);
		// double end = System.currentTimeMillis();
		// System.out.println("Time: " + ((end - start) / 1000));
		// Map<String, Object> cleanHash = new HashMap<String, Object>();
		// Map<String, Object> rawHash = new HashMap<String, Object>();
		// cleanHash.put("Capital", "A0");
		// cleanHash.put("Lowercase", "a0");
		// cleanHash.put("Number", "0");
		// rawHash.put("Capital", "http://semoss.org/ontologies/Concept/Capital/A0");
		// rawHash.put("Lowercase", "http://semoss.org/ontologies/Concept/Lowercase/a0");
		// rawHash.put("Number", "http://semoss.org/ontologies/Concept/Number/0");
		// // secondTree.addRow(cleanHash, rawHash);
		// System.out.println("****NEW TREE****");
		// // Iterator<Object> capIterator = secondTree.uniqueValueIterator("Capital", false, true); // Change for each query
		// // while (capIterator.hasNext()) {
		// // System.out.println(capIterator.next().toString());
		// // }
		// // Iterator<Object> lowerIterator = secondTree.uniqueValueIterator("Lowercase", false, true); // Change for each query
		// // while (lowerIterator.hasNext()) {
		// // System.out.println(lowerIterator.next().toString());
		// // }
		// // Iterator<Object> numIterator = secondTree.uniqueValueIterator("Number", false, true); // Change for each query
		// // while (numIterator.hasNext()) {
		// // System.out.println(numIterator.next().toString());
		// // }

		System.out.println("done");
//		String fileName = "C:\\Users\\bisutton\\Desktop\\BTreeTester.xlsx";
//		String fileName2 = "C:\\Users\\bisutton\\Desktop\\BTreeTester2.xlsx";
//		String fileName3 = "C:\\Users\\bisutton\\Desktop\\BTreeTester3.xlsx";
//		String fileNameout = "C:\\Users\\bisutton\\Desktop\\BTreeOut.xlsx";
		
//		testSerializingAndDeserialing(fileName);
//		testAppend(fileName, fileName2, fileNameout);
//		testStoringAndWriting(fileName, fileNameout);
		//testJoin(fileName, fileName2, fileName3, fileNameout);


//		String[] columnHeaders = tree.getColumnHeaders();
//		boolean[] numeric = tree.isNumeric();
//		DuplicationReconciliation.ReconciliationMode[] array = new DuplicationReconciliation.ReconciliationMode[4];
//		array[0] = DuplicationReconciliation.ReconciliationMode.COUNT;
//		array[1] = DuplicationReconciliation.ReconciliationMode.MEAN;
//		array[2] = DuplicationReconciliation.ReconciliationMode.MEDIAN;
//		array[3] = DuplicationReconciliation.ReconciliationMode.COUNT;
//		
//		long startTime = System.currentTimeMillis();
//		TreeNode root = tree.simpleTree.nodeIndexHash.get("Source_Integer");
//		CompressionIterator iterator = new CompressionIterator(root, array);
//		List<Object[]> data = new ArrayList<Object[]>();
//		while(iterator.hasNext()) {
//			data.add(iterator.next());
//		}
//		System.out.println(System.currentTimeMillis() - startTime);
		
//		startTime = System.currentTimeMillis();
//		root = tree.simpleTree.nodeIndexHash.get("Source_Integer");
//		iterator = new CompressionIterator(root, array);
//		data = new ArrayList<Object[]>();
//		while(iterator.hasNext()) {
//			data.add(iterator.next());
//		}
//		System.out.println(System.currentTimeMillis() - startTime);
		
//		System.out.println("Start Median Test");
//		for(int i = 0; i < columnHeaders.length; i++) {
//			if(numeric[i]) {
//				long startTime = System.currentTimeMillis();
//				Double d = DuplicationReconciliation.getMedian(tree.getColumn(columnHeaders[i]), true);
//				System.out.println("time for column "+columnHeaders[i]+": "+(System.currentTimeMillis() - startTime));
//				System.out.println(columnHeaders[i]+ " Avg: "+d);
//			}
//		}
//		System.out.println("End Median Test\n");
//		
//		System.out.println("Start Mean Test");
//		for(int i = 0; i < columnHeaders.length; i++) {
//			if(numeric[i]) {
//				long startTime = System.currentTimeMillis();
//				Double d = DuplicationReconciliation.getMean(tree.getColumn(columnHeaders[i]), true);
//				System.out.println("time for column "+columnHeaders[i]+": "+(System.currentTimeMillis() - startTime));
//				System.out.println(columnHeaders[i]+ " Avg: "+d);
//			}
//		}
//		System.out.println("End Mean Test\n");
//		
//		System.out.println("Start Max Test");
//		for(int i = 0; i < columnHeaders.length; i++) {
//			if(numeric[i]) {
//				long startTime = System.currentTimeMillis();
//				Double d = DuplicationReconciliation.getMax(tree.getColumn(columnHeaders[i]), true);
//				System.out.println("time for column "+columnHeaders[i]+": "+(System.currentTimeMillis() - startTime));
//				System.out.println(columnHeaders[i]+ " Avg: "+d);
//			}
//		}
//		System.out.println("End Max Test\n");
//		
//		System.out.println("Start Mode Test");
//		for(int i = 0; i < columnHeaders.length; i++) {
//			if(numeric[i]) {
//				long startTime = System.currentTimeMillis();
//				Double d = DuplicationReconciliation.getMode(tree.getColumn(columnHeaders[i]), true);
//				System.out.println("time for column "+columnHeaders[i]+": "+(System.currentTimeMillis() - startTime));
//				System.out.println(columnHeaders[i]+ " Avg: "+d);
//			}
//		}
//		System.out.println("End Mode Test\n");
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
			serialized = node.serializeTree(node);
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
		
		String sheetName = fileName.substring(0, fileName.length() - 5);
		sheetName = fileName.substring(74);
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
				//String v1 = row.getCell(cIndex).getStringCellValue();
				double v1 = row.getCell(cIndex).getNumericCellValue();
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
			//System.out.println("wrote row " + i);
		}
		//System.out.println("wrote file " + fileNameout);
		
		Utility.writeWorkbook(workbookout, fileNameout);
		//testnum++;
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {

		long start = System.currentTimeMillis();
		System.err.println("start : " + start);
		processPreTransformations(component, component.getPreTrans());

		System.err.println("finished pre comps : " + (System.currentTimeMillis() - start));
		IEngine engine = component.getEngine();
		// automatically created the query if stored as metamodel
		// fills the query with selected params if required
		// params set in insightcreatrunner
		String query = component.fillQuery();
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		System.err.println("finished query running : " + (System.currentTimeMillis() - start));
		String[] displayNames = wrapper.getDisplayVariables(); // pulled this outside of the if/else block on purpose. 
		ITableDataFrame newDataFrame = null;
		if(this.levelNames == null){
			setLevelNames(displayNames);
			while(wrapper.hasNext()){
				this.addRow(wrapper.next());
			}
		}
		else {
			newDataFrame = wrapper.getTableDataFrame();
		}
		System.err.println("finished component processing : " + (System.currentTimeMillis() - start));

		processPostTransformations(component, component.getPostTrans(), newDataFrame);
		
		processActions(component, component.getActions());
		System.err.println("finished post : " + (System.currentTimeMillis() - start));
	}
	
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms){
		LOGGER.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}
	
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame){
		LOGGER.info("We are processing " + transforms.size() + " post transformations");
		// if other data frames present, create new array with this at position 0
		IDataMaker[] extendedArray = new IDataMaker[]{this};
		if(dataFrame.length > 0) {
			extendedArray = new IDataMaker[dataFrame.length + 1];
			extendedArray[0] =  this;
			for(int i = 0; i < dataFrame.length; i++) {
				extendedArray[i+1] = dataFrame[i];
			}
		}
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
//			this.join(dataFrame, transform.getOptions().get(0).getSelected()+"", transform.getOptions().get(1).getSelected()+"", 1.0, (IAnalyticRoutine)transform);
//			LOGGER.info("welp... we've got our new table... ");
		}
	}
	
	@Override
	public List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker) {
		LOGGER.info("We are processing " + actions.size() + " actions");
		List<Object> outputs = new ArrayList<Object>();
		for(ISEMOSSAction action : actions){
			action.setDataMakers(this);
			action.setDataMakerComponent(dmc);
			outputs.add(action.runMethod());
		}
		algorithmOutput.addAll(outputs);
		return outputs;
	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
		Hashtable retHash = new Hashtable();
		retHash.put("data", this.getRawData());
		retHash.put("headers", this.levelNames);
		return retHash;
	}

	@Override
	public List<Object> getActionOutput() {
		return this.algorithmOutput;
	}
	
	public void binNumericColumn(String column) {
		if(!this.isNumeric(column)) {
			throw new IllegalArgumentException("Column must be numeric in order to bin");
		}
		
		Double[] values = this.getColumnAsNumeric(column);
		BarChart chart = new BarChart(values);
		String[] binValues = chart.getAssignmentForEachObject();

		String[] names = new String[2];
		names[0] = column;
		names[1] = column + BIN_COL_NAME_ADDED;
		ITableDataFrame table = new BTreeDataFrame(names);
		
		Map<String, Object> addBinValues = new HashMap<String, Object>();
		for(int j = 0; j < values.length; j++) {
			addBinValues.put(names[0], values[j]);
			addBinValues.put(names[1], binValues[j]);
			table.addRow(addBinValues, addBinValues);
		}
		
		this.join(table, column, column, 1.0, new ExactStringMatcher());
		binMap.put(column, column + BIN_COL_NAME_ADDED);
	}

	@Override
	public void binNumericalColumns(String[] columns) {
		for(String col : columns) {
			binNumericColumn(col);
		}
	}
	
	@Override
	public void binAllNumericColumns() {
		boolean[] isNumeric = isNumeric();
		int curLevelCount = filteredLevelNames.length;
		for(int i = 0; i < curLevelCount; i++) {
			if(isNumeric[i] && !binMap.containsKey(filteredLevelNames[i])) {
				binNumericColumn(filteredLevelNames[i]);
			}
		}
	}
	
	public void printTree() {
		
		IndexTreeIterator iterator = new IndexTreeIterator(this.simpleTree.nodeIndexHash.get(levelNames[0]));
		while(iterator.hasNext()) {
			TreeNode nextNode = iterator.next();
			//System.out.print(nextNode.getValue());
			Vector<SimpleTreeNode> nodes = nextNode.getAllInstances();
			//printM(nextNode);
			printBranch(nodes.get(0), 0, null);
		}
	}
	
	private void printBranch(SimpleTreeNode node, int n, Boolean left) {
		if(node == null) return;
		
		String side = "RIGHT";
		if(left != null && left) side = "LEFT";
		
		printTabs(n);
		if(left == null) {
			System.out.print(node.leaf.getValue());
		}
		else {
			System.out.print(side+":"+node.leaf.getValue());
		}
		printM(node);
		printBranch(node.leftChild, n+1, true);
		printBranch(node.rightChild, n+1, false);
		if(left!=null)	printBranch(node.rightSibling, n, left);
	}
	
	private void printTabs(int n) {
		for(int i = 0; i < n; i++)
		System.out.print("\t");
	}
	
	private void printM(SimpleTreeNode n) {
		TreeNode foundNode = this.simpleTree.getNode((ISEMOSSNode)n.leaf);
		if(foundNode.instanceNode.contains(n)) {
			System.out.print(" (UM) ");
		} else if(foundNode.filteredInstanceNode.contains(n)) {
			System.out.print(" (M) ");
		} else if(foundNode.transFilteredInstanceNode.contains(n)){
			System.out.print(" (TM) ");
		} else {
			System.out.print(" NOT FOUND");
		}
		
		System.out.println(n.hardFiltered+" "+n.transitivelyFiltered);
	}
	
	private void printM(TreeNode n) {
		TreeNode foundNode = this.simpleTree.getNode((ISEMOSSNode)n.leaf);
		if(n.instanceNode.size() > 0) {
			System.out.println(" (UM)");
		} else if(foundNode.filteredInstanceNode.size()> 0) {
			System.out.println(" (M)");
		} else if(foundNode.transFilteredInstanceNode.size()> 0){
			System.out.println(" (TM)");
		} else {
			System.out.println(" NOT FOUND");
		}
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void save(String fileName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mergeEdgeHash(Map<String, Set<String>> primKeyEdgeHash, Map<String, String> dataType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void connectTypes(String outType, String inType, Map<String, String> dataType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Set<String>> getEdgeHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getEnginesForUniqueName(String sub) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPhysicalUriForNode(String string, String engineName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getTableHeaderObjects() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void connectTypes(String[] joinCols, String newCol, Map<String, String> dataType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addRow(Object[] cleanCells, Object[] rawCells, String[] headers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine,
			Vector<Map<String, String>> joinCols) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData,
			Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues,
			String comparator) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow,
			Map<String, Object> rawRow) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Object[] rawValues,
			Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDerivedColumn(String uniqueName, boolean isDerived) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDerviedCalculation(String uniqueName, String calculationName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDerivedUsing(String uniqueName, String... otherUniqueNames) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.TinkerColAddReactor");
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.GDMImportDataReactor");
//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		switch(reactorType) {
//			case IMPORT_DATA : return new GDMImportDataReactor();
//			case COL_ADD : return new ColAddReactor();
//		}
		
		return reactorNames;
	}

	@Override
	public DATA_TYPES getDataType(String uniqueName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setUserId(String userId) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String getUserId() {
		return null;
	}

	@Override
	public String getDataMakerName() {
		return "BTreeDataFrame";
	}
}
