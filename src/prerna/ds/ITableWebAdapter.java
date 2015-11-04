package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;

public class ITableWebAdapter {
	
	private ITableWebAdapter() {
		
	}
	
	public static List<HashMap<String, Object>> getData(ITableDataFrame table) {
		BTreeDataFrame btable = (BTreeDataFrame)table;
		String[] columnHeaders = btable.getColumnHeaders();
		String lastColumn = columnHeaders[columnHeaders.length - 1];
		TreeNode root = btable.getBuilder().nodeIndexHash.get(lastColumn);
		
		String[] skipColumns = btable.getFilteredColumns();
		List<String> skipCols = new ArrayList<String>();
		for(String s : skipColumns) {
			skipCols.add(s);
		}
		
		WebBTreeIterator iterator = new WebBTreeIterator(root,"", true, skipCols);
		List<HashMap<String, Object>> retData = new ArrayList<HashMap<String, Object>>();
		while(iterator.hasNext()) {
			retData.addAll(iterator.next());
		}
		return retData;
	}
}
