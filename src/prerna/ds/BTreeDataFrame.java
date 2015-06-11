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
			row[index] = createNodeObject(val, rawVal, levelNames[index]);

		}
		simpleTree.addNodeArray(row);
	}

	private ISEMOSSNode createNodeObject(Object value, Object rawValue, String level) {
		ISEMOSSNode node;

		if(value == null) {
			node = new StringClass(null, level); // TODO: fix this
		} else if(value instanceof Integer) {
			node = new IntClass((int)value, rawValue.toString(), level);
		} else if(value instanceof Number) {
			node = new DoubleClass((double)value, rawValue.toString(), level);
		} else if(value instanceof String) {
			System.out.println(rawValue);
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
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		if(typeRoot == null){
			return null;
		}
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		List<Object[]> table = new ArrayList<Object[]>();
		leftRootNode.flattenTreeFromRoot(leftRootNode, new Vector<Object>(), table, levelNames.length);

		return table;
	}
	
	@Override
	public List<Object[]> getRawData() {
		TreeNode typeRoot = simpleTree.nodeIndexHash.get(levelNames[0]);
		SimpleTreeNode leftRootNode = typeRoot.getInstances().elementAt(0);
		leftRootNode = leftRootNode.getLeft(leftRootNode);

		List<Object[]> table = new ArrayList<Object[]>();
		leftRootNode.flattenRawTreeFromRoot(leftRootNode, new Vector<Object>(), table, levelNames.length);

		return table;
	}

	@Override
	public List<String> getMostSimilarColumns(ITableDataFrame table, double confidenceThreshold, IAnalyticRoutine routine) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IAnalyticRoutine routine) 
			throws Exception {
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
		if(matched == null){
			throw new Exception("No matching elements found");
		}

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
//			Iterator<Object[]> matchedIterator = matched.iterator();

			TreeNode thisRootNode = this.simpleTree.nodeIndexHash.get(colNameInTable); //TODO: is there a better way to get the type? I don't think this is reliable
			Vector thisSearchVector = new Vector();
			thisSearchVector.addElement(thisRootNode);

			SimpleTreeBuilder passedBuilder = passedTree.getBuilder();
			Map<String, TreeNode> newIdxHash = new HashMap<String, TreeNode>();
			TreeNode passedRootNode = passedBuilder.nodeIndexHash.remove(colNameInJoiningTable); //TODO: is there a better way to get the type? I don't think this is reliable
			Vector passedSearchVector = new Vector();
			passedSearchVector.addElement(passedRootNode);
			
			// Iterate for every row in the matched table
			for(Object[] flatMatchedRow : flatMatched) { // for each matched item
				Object item1 = flatMatchedRow[0];
				Object item2 = flatMatchedRow[1];
//			while(matchedIterator.hasNext()){
//				Object[] flatMatchedRow = matchedIterator.next();
//				Object item1 = flatMatchedRow[0];
//				Object item2 = flatMatchedRow[1];
				
				System.out.println(item1 + "           " + item2);

				// search for tree node in this table and tree node in passed table
				TreeNode thisSearchNode = new TreeNode(createNodeObject(item1, item1, colNameInTable)); //TODO: how do we generically do this...?
				TreeNode thisTreeNode = thisRootNode.getNode(thisSearchVector, thisSearchNode, false);
				Vector <SimpleTreeNode> thisInstances = thisTreeNode.getInstances();
//				System.out.println(thisInstances.size());
				
				TreeNode passedSearchNode = new TreeNode(createNodeObject(item2, item2, colNameInJoiningTable)); //TODO: how do we generically do this...?
				TreeNode passedTreeNode = passedRootNode.getNode(passedSearchVector, passedSearchNode, false);
				Vector <SimpleTreeNode> passedInstances = passedTreeNode.getInstances();
//				System.out.println(passedInstances.size()); // this should be 1 since right now we are assuming roots
				SimpleTreeNode instance2HookUp = passedInstances.get(0).leftChild;

				String serialized = "";
				Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
				vec.add(instance2HookUp);
				serialized = instance2HookUp.serializeTree("", vec, true, 0);
//				System.out.println("SERIALIZED " + instance2HookUp.leaf.getKey() + " AS " + serialized);
					
				// hook up passed tree node with each instance of this tree node
				for(int instIdx = 0; instIdx < thisInstances.size(); instIdx++){
					SimpleTreeNode myNode = thisInstances.get(instIdx);
					SimpleTreeNode hookUp = instance2HookUp.deserializeTree(serialized, newIdxHash);//
					SimpleTreeNode.addLeafChild(myNode, hookUp);
//					myNode.leftChild = hookUp;
//					while(hookUp!=null){
//						SimpleTreeNode.addLeafChild(myNode, hookUp);
//						hookUp.parent = myNode;
//						hookUp = hookUp.rightSibling;
//					}
//					System.out.println("joining " + myNode.leaf.getKey() + " with " + hookUp.leaf.getKey());
				}
			}
			this.simpleTree.nodeIndexHash.putAll(newIdxHash);
			
		}
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
		
		//Determine if this BTreeDataFrame is structurally the same as bTree arguments
		String[] tableHeaders = bTree.getColumnHeaders();
		boolean same = true;
		if(tableHeaders.length == levelNames.length) {
			for(int i = 0; i < levelNames.length; i++) {
				if(!levelNames[i].equals(tableHeaders[i])) {
					same = false; break;
				}
			}
		}
		
		if(same) {
			simpleTree.append(this.simpleTree.getRoot(), bTree.getTree().getRoot());
		} else {
			//do something else which I don't know yet
		}
	}
	private void mergeBranches(ISEMOSSNode node) {
		
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
		typeRoot = typeRoot.getLeft(typeRoot);
		Iterator<Object[]> it = new BTreeIterator(typeRoot);
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
	public Object[] getRawColumn(String columnHeader) {

		TreeNode typeRoot = simpleTree.nodeIndexHash.get(columnHeader);
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
		for(String name : levelNames){
			if (count >= newNames.length) { // this means a column header was passed in that doesn't exist in the tree
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
		String fileName = "C:\\Users\\bisutton\\Desktop\\BTreeTester.xlsx";
		String fileName2 = "C:\\Users\\bisutton\\Desktop\\BTreeTester2.xlsx";
		String fileName3 = "C:\\Users\\bisutton\\Desktop\\BTreeTester3.xlsx";
		String fileNameout = "C:\\Users\\bisutton\\Desktop\\BTreeOut.xlsx";
		
//		testSerializingAndDeserialing(fileName);
//		testAppend(fileName, fileName2, fileNameout);
//		testStoringAndWriting(fileName, fileNameout);
		testJoin(fileName, fileName2, fileName3, fileNameout);
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
				
			SimpleTreeNode hookUp = node.deserializeTree(serialized, tester.simpleTree.nodeIndexHash);//
			
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
			tester.join(joiner, names1[names1.length-2], names2[0], 1, new ExactStringMatcher());
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
	}
}
