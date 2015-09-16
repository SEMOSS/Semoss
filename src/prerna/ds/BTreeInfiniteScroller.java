package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BTreeInfiniteScroller implements InfiniteScroller {

	private BTreeDataFrame table;
	private int returnCount;
	private Map<String, Iterator<TreeNode>> iteratorMap;
	private Iterator<List<HashMap<String, Object>>> rowIterator;
	private String lastColumn;
	private String iteratorColumn;
	private int currentCount;
	private final static String VALUE = "value";
	private final static String SELECTED = "selected";
	
	public BTreeInfiniteScroller(BTreeDataFrame table) {
		this.table = table;
		returnCount = 100;
		iteratorMap = new HashMap<String, Iterator<TreeNode>>();
		String[] columns = table.getColumnHeaders();
		lastColumn = columns[columns.length-1];
		iteratorColumn = lastColumn;
		rowIterator = new WebBTreeIterator(table.getBuilder().nodeIndexHash.get(lastColumn));
		currentCount = 0;
	}
	
	public void resetIterators() {
		iteratorMap.clear();
	}
	
	public void resetTable() {
		
		//Check to see if the table has changed
		String[] columnHeaders = table.getColumnHeaders();
		int lastIndex = columnHeaders.length - 1;
		if(!columnHeaders[lastIndex].equalsIgnoreCase(lastColumn)) {
			lastColumn = columnHeaders[lastIndex];
		}
		
		rowIterator = new WebBTreeIterator(table.getBuilder().nodeIndexHash.get(lastColumn), false);
		currentCount = 0;
	}
	
	public List<HashMap<String, String>> getNextUniqueValues(String columnHeader) {
		
		if(!iteratorMap.containsKey(columnHeader)) {
			TreeNode root = table.getBuilder().nodeIndexHash.get(columnHeader);
			iteratorMap.put(columnHeader, new IndexTreeIterator(root));
		}
		
		Iterator<TreeNode> iterator = iteratorMap.get(columnHeader);
		List<HashMap<String, String>> returnList = new ArrayList<HashMap<String, String>>();
		int count = 0;
		while(iterator.hasNext() && count < returnCount) {
			HashMap<String, String> nextMap = new HashMap<String, String>();
			TreeNode t = iterator.next();
			String selected;
			if(t.instanceNode.size() > 0) {
				selected = "true";
			} else if(t.filteredInstanceNode.size() > 0){
				selected = "false";
			} else {
				continue;
			}
			String value = iterator.next().leaf.getRawValue().toString();
			nextMap.put(VALUE, value);
			nextMap.put(SELECTED, selected);
			returnList.add(nextMap);
		}
		return returnList;
	}
	
//	public List<HashMap<String, Object>> getData(int start, int end) {
//		return null;
//	}
	
	public List<HashMap<String, Object>> getNextData(String column) {
		
		if(column == null) column = iteratorColumn;
		//Check to see if the table has changed or column has changed
		String[] columnHeaders = table.getColumnHeaders();
		int lastIndex = columnHeaders.length - 1;
		if(!columnHeaders[lastIndex].equalsIgnoreCase(lastColumn) || !column.equalsIgnoreCase(iteratorColumn)) {
			iteratorColumn = column;
			lastColumn = columnHeaders[lastIndex];
			ArrayList<String> columns2skip = new ArrayList<String>(columnHeaders.length);
			for(String column2skip : table.getColumnsToSkip()) {
				columns2skip.add(column2skip);
			}
			rowIterator = new WebBTreeIterator(table.getBuilder().nodeIndexHash.get(column), false, columns2skip);
		}
		
		//create the data in the format the front end expects
		ArrayList<HashMap<String, Object>> nextData = new ArrayList<HashMap<String, Object>>();
		while(nextData.size() < 100 && rowIterator.hasNext()) {
			nextData.addAll(rowIterator.next());
		}
//		HashMap<String, Object> row;
//		for(int i = 0; i < returnCount && rowIterator.hasNext(); i++) {
//			row = new HashMap<String, Object>();
//			TreeNode node = rowIterator.next();
//			while(node != null) {
//				ISEMOSSNode sNode = ((ISEMOSSNode)node.leaf);
//				row.put(sNode.getType(), sNode.getRawValue());
//				node = node.parent;
//			}
//			nextData.add(row);
//			currentCount++;
//		}
		return nextData;
	}
}
