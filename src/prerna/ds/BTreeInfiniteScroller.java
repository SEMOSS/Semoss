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
	private Iterator<TreeNode> rowIterator;
	private String lastColumn;
	private final static String VALUE = "value";
	private final static String SELECTED = "selected";
	
	public BTreeInfiniteScroller(BTreeDataFrame table) {
		this.table = table;
		returnCount = 100;
		iteratorMap = new HashMap<String, Iterator<TreeNode>>();
		rowIterator = new IndexTreeIterator(table.getBuilder().nodeIndexHash.get(lastColumn));
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
		
		rowIterator = new FilteredIndexTreeIterator(table.getBuilder().nodeIndexHash.get(lastColumn));
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
	
	public List<HashMap<String, Object>> getNextData() {
		
		//Check to see if the table has changed
		String[] columnHeaders = table.getColumnHeaders();
		int lastIndex = columnHeaders.length - 1;
		if(!columnHeaders[lastIndex].equalsIgnoreCase(lastColumn)) {
			lastColumn = columnHeaders[lastIndex];
			rowIterator = new FilteredIndexTreeIterator(table.getBuilder().nodeIndexHash.get(lastColumn));
		}
		
		//create the data in the format the front end expects
		ArrayList<HashMap<String, Object>> nextData = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> row;
		for(int i = 0; i < returnCount && rowIterator.hasNext(); i++) {
			row = new HashMap<String, Object>();
			TreeNode node = rowIterator.next();
			while(node != null) {
				ISEMOSSNode sNode = ((ISEMOSSNode)node.leaf);
				row.put(sNode.getType(), sNode.getRawValue());
				node = node.parent;
			}
			nextData.add(row);
		}
		return nextData;
	}
}
