package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BTreeInfiniteScroller implements InfiniteScroller {

	private BTreeDataFrame table;
	private static final int returnCount = 100;
	private Iterator<List<HashMap<String, Object>>> rowIterator;
	private String iteratorColumn;
	private int currentCount;
	
	private List<HashMap<String, Object>> rowBuffer;
//	private Map<String, Iterator<TreeNode>> iteratorMap;
	
	public BTreeInfiniteScroller(BTreeDataFrame table) {
		this.table = table;
		iteratorColumn = table.getColumnHeaders()[0];
		resetTable();
		
//		iteratorMap = new HashMap<String, Iterator<TreeNode>>();
	}
	
	public void resetIterators() {
//		iteratorMap.clear();
	}
	
	public void resetTable() {	
		String[] columnHeaders = table.getColumnHeaders();
		ArrayList<String> columns2skip = new ArrayList<String>(columnHeaders.length);
		for(String column2skip : table.getColumnsToSkip()) {
			columns2skip.add(column2skip);
		}
		rowIterator = new WebBTreeIterator(table.getBuilder().nodeIndexHash.get(iteratorColumn), false, columns2skip);
		rowBuffer = new ArrayList<HashMap<String, Object>>();
		currentCount = 0;
	}
	
	public List<HashMap<String, String>> getNextUniqueValues(String columnHeader) {
		
//		if(!iteratorMap.containsKey(columnHeader)) {
//			TreeNode root = table.getBuilder().nodeIndexHash.get(columnHeader);
//			iteratorMap.put(columnHeader, new IndexTreeIterator(root));
//		}
//		
//		Iterator<TreeNode> iterator = iteratorMap.get(columnHeader);
//		List<HashMap<String, String>> returnList = new ArrayList<HashMap<String, String>>();
//		int count = 0;
//		while(iterator.hasNext() && count < returnCount) {
//			HashMap<String, String> nextMap = new HashMap<String, String>();
//			TreeNode t = iterator.next();
//			String selected;
//			if(t.instanceNode.size() > 0) {
//				selected = "true";
//			} else if(t.filteredInstanceNode.size() > 0){
//				selected = "false";
//			} else {
//				continue;
//			}
//			String value = iterator.next().leaf.getRawValue().toString();
//			nextMap.put(VALUE, value);
//			nextMap.put(SELECTED, selected);
//			returnList.add(nextMap);
//		}
//		return returnList;
		return null;
	}

	
	public List<HashMap<String, Object>> getNextData(String column, int start, int end) {
		
		List<HashMap<String, Object>> nextData = new ArrayList<HashMap<String, Object>>();
		
		//If a new column is used to base sorting off of, reset with new column
		if(column != null && !column.equals(iteratorColumn)) {
			iteratorColumn = column;
			resetTable();
		}
		
		//if iterator is ahead of the start point
		if(currentCount < start) {
			
			resetTable();
			nextData = this.getNextData(column, 0, end);
			nextData = nextData.subList(start, end);
		
		} else if(currentCount > start) {
			
		} else {
		
			if(rowBuffer.size() > returnCount) {
				
				nextData = rowBuffer.subList(0, returnCount);
				rowBuffer = rowBuffer.subList(returnCount, rowBuffer.size());
			
			} else {
				
				nextData.addAll(rowBuffer);
				rowBuffer.clear();
				while(nextData.size() < returnCount && rowIterator.hasNext()) {
					nextData.addAll(rowIterator.next());
				}
			
				if(nextData.size() > returnCount) {
					rowBuffer = nextData.subList(returnCount, nextData.size());
					nextData = nextData.subList(0, returnCount);
				}	
			}			
			currentCount += end;
		}

		return nextData;
	}
}
