package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.ArrayUtilityMethods;

public class TableDataFrameWebAdapter {
	
	private static final Logger LOGGER = LogManager.getLogger(TableDataFrameWebAdapter.class.getName());
	
	private TableDataFrameWebAdapter() {
		
	}
	
	/**
	 * 
	 * @param table - the dataframe to extract data from
	 * @return  - formatted data in an optimal form for the Front End
	 */
	public static List<HashMap<String, Object>> getData(ITableDataFrame table) {
		
		//Cast to Btree
		BTreeDataFrame btable = (BTreeDataFrame)table;
		
		//get the root of the index tree for the last column
		String[] columnHeaders = btable.getColumnHeaders();
		String lastColumn = columnHeaders[columnHeaders.length - 1];
		TreeNode root = btable.getBuilder().nodeIndexHash.get(lastColumn);
		
		//figure out which columns to skip
		String[] skipColumns = btable.getFilteredColumns();
		List<String> skipCols = new ArrayList<String>();
		for(String s : skipColumns) {
			skipCols.add(s);
		}
		
		//use the iterator specific for returning data for the front end
		WebBTreeIterator iterator = new WebBTreeIterator(root,"", true, skipCols);
		
		//List of Hashmaps is optimal for front end to reduce looping 
		List<HashMap<String, Object>> retData = new ArrayList<HashMap<String, Object>>();
		while(iterator.hasNext()) {
			retData.addAll(iterator.next());
		}
		return retData;
	}
	
	public static List<HashMap<String, Object>> getData(ITableDataFrame tableData, String concept, String sortType, int startRow, int endRow) {

		// Cast to B-tree, table from which data is being extracted
		BTreeDataFrame table = (BTreeDataFrame) tableData; 

		// figure out which columns to skip for the return data
		String[] skipColumns = table.getFilteredColumns();
		List<String> skipCols = new ArrayList<String>();
		for (String s : skipColumns) {
			skipCols.add(s);
		}

		// sort should either be "desc" or "asc", if sort is null then set sort
		// to "asc"
		String sort = "asc"; //sorting, asc for ascending, desc for descending
		if (!sort.equalsIgnoreCase(sortType)) {
			sort = sortType;
		}
		
		// if column is null, column should equal first column in table
		String[] columnHeaders = table.getColumnHeaders();
		String iteratorColumn = null; //the column on which to sort on
		if (concept != null && ArrayUtilityMethods.arrayContainsValueIgnoreCase(columnHeaders, concept)) {
			iteratorColumn = concept;
		} else {
			//default to first column
			iteratorColumn = columnHeaders[0];
		}
		TreeNode root = table.getBuilder().nodeIndexHash.get(iteratorColumn);
		
		// use the iterator specific for returning data for the front end
		WebBTreeIterator rowIterator = new WebBTreeIterator(root, sort, true, skipCols);

		endRow = endRow + 1;		

		//gather the necessary return data
		List<HashMap<String, Object>> nextData = new ArrayList<HashMap<String, Object>>();
		while (nextData.size() <= endRow && rowIterator.hasNext()) {
			nextData.addAll(rowIterator.next());
		}

		//if size is less than end row, increment end row by 1
		if (nextData.size() < endRow) {
			endRow = nextData.size() + 1;
		}

		//if empty return empty list of hashmaps
		if (nextData.size() == 0) {
			return nextData;
		}

		try {
			nextData = nextData.subList(startRow, endRow);
		} catch (Exception e) {
			nextData = nextData.subList(startRow, --endRow);
		}
		return  nextData;
	}
	
	public static Object[] getRawFilterModel(ITableDataFrame table) {
		
		//Cast to Btree
		BTreeDataFrame btable = (BTreeDataFrame)table;
		
		HashMap<String, Object[]> visibleValues = new HashMap<String, Object[]>();
		HashMap<String, Object[]> invisibleValues = new HashMap<String, Object[]>();
		
		String[] levelNames = btable.getColumnHeaders();
		
		String root = levelNames[0];
		visibleValues.put(root, btable.getUniqueRawValues(root));
		invisibleValues.put(root, btable.getFilteredUniqueRawValues(root));
		
		for(int i = 0; i < levelNames.length - 1; i++) {
			String column = levelNames[i];
			
			String grabColumn = levelNames[i+1];
			visibleValues.put(grabColumn, btable.getUniqueRawValues(grabColumn));
			
			
			ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(btable.simpleTree.nodeIndexHash.get(column));
			Set<String> invisibleValueSet = new HashSet<String>();
			
			while(iterator.hasNext()) {
				SimpleTreeNode nextParent = iterator.next();
				
				SimpleTreeNode leftChild = nextParent.leftChild;
				SimpleTreeNode rightChild = nextParent.rightChild;
				
				if(leftChild != null) {
					while(rightChild != null) {
						invisibleValueSet.add(rightChild.leaf.getRawValue().toString());
						rightChild = rightChild.rightSibling;
					}
				}
			}
			
			invisibleValues.put(grabColumn, invisibleValueSet.toArray());
		}
		
		return new Object[]{visibleValues, invisibleValues};
	}
	
}
