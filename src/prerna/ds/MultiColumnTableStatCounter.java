package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.util.ArrayUtilityMethods;

public class MultiColumnTableStatCounter {

	private static final String AVG = "average";
	private static final String MAX = "max";
	private static final String MIN = "min";
	private static final String MEDIAN = "median";
	private static final String MODE = "mode";
	private static final String COUNT = "count";
	
	private BTreeDataFrame table; //the table being operated on/from
	private Map<String, Object> functionMap; //the map that indicates which column should be evaluated with which math function
	private String[] origColHeaders; //the original column headers from the table
	private String[] columnHeaders; //the column headers in which the group by is based on
	
	public MultiColumnTableStatCounter() {
		
	}
	
	//setting the variables
	private void setVariables(ITableDataFrame table, String[] columnHeaders, Map<String, Object> functionMap) {
		if(!(table instanceof BTreeDataFrame)) {
			throw new IllegalArgumentException("only table types of BTreeDataFrame supported");
		}
		
		this.origColHeaders = table.getColumnHeaders();
		//arrange column Headers in order
		
		//inefficient but will fix later, need to test new join logic
		Map<Integer, String> orderMap = new HashMap<Integer, String>();
		for(String header : columnHeaders) {
			int x = ArrayUtilityMethods.calculateIndexOfArray(origColHeaders, header);
			orderMap.put(x, header);
		}
		
		String[] orderedColumnHeaders = new String[columnHeaders.length];
		int x = 0;
		for(int i = 0; i < origColHeaders.length; i++) {
			if(orderMap.containsKey(i)) {
				orderedColumnHeaders[x] = orderMap.get(i);
				x++;
			}
		}
		
		this.columnHeaders = orderedColumnHeaders;
		this.table = (BTreeDataFrame)table;
		this.functionMap = functionMap;
	}
	
	public void addStatsToDataFrame(ITableDataFrame table, String[] columnHeaders, Map<String, Object> functionMap) {
		setVariables(table, columnHeaders, functionMap);
		
		DuplicationReconciliation[] duprec = getDuplicationReconciliation();
		Map<String, String> columnHeaderMap = getNewColumnHeaders();
		BTreeDataFrame newTable = createNewBTree(columnHeaderMap, duprec);
		
		
		this.table.join(newTable, this.columnHeaders, this.columnHeaders, 1.0);
	}
	
	private DuplicationReconciliation[] getDuplicationReconciliation() {
		
//		DuplicationReconciliation[] array = new DuplicationReconciliation[functionMap.keySet().size()+1];
		List<DuplicationReconciliation> array = new ArrayList<DuplicationReconciliation>();
		
		Map<String, DuplicationReconciliation> duprecMap = new HashMap<>(origColHeaders.length);
		
		
//		int i = 0;
		for(String key : functionMap.keySet()) {
		
			Map<String, String> mathMap = (Map<String, String>)functionMap.get(key);
			String operation = mathMap.get("math");
			DuplicationReconciliation.ReconciliationMode mode; 
			switch(operation.toLowerCase()) {
				case AVG : mode = DuplicationReconciliation.ReconciliationMode.MEAN; break;
					
				case MAX : mode = DuplicationReconciliation.ReconciliationMode.MAX; break;
					
				case MIN : mode = DuplicationReconciliation.ReconciliationMode.MIN; break;
					
				case MEDIAN : mode = DuplicationReconciliation.ReconciliationMode.MEDIAN; break;
				 
				case MODE : mode = DuplicationReconciliation.ReconciliationMode.MODE; break;
				
				case COUNT : mode = DuplicationReconciliation.ReconciliationMode.COUNT; break;
				
				default : mode = DuplicationReconciliation.ReconciliationMode.MEAN; break;
			}
//			array[i] = new DuplicationReconciliation(mode);
			duprecMap.put(mathMap.get("name"), new DuplicationReconciliation(mode));
//			i++;
		}
		
		for(String c : origColHeaders) {
			if(ArrayUtilityMethods.arrayContainsValue(columnHeaders, c)) {
				array.add(null);
			} else {
				DuplicationReconciliation dr = duprecMap.get(c);
				if(dr != null) {
					array.add(duprecMap.get(c));
				}
			}
		}
		
		return array.toArray(new DuplicationReconciliation[array.size()]);
	}
	
	private BTreeDataFrame createNewBTree(Map<String, String> columnHeaderMap, DuplicationReconciliation[] mathModes) {
//		String[] newColHeaders = new String[columnHeaderMap.keySet().size()];
		ArrayList<String> newHeaders = new ArrayList<String>();
		int i;
		for(i = 0; i < columnHeaders.length; i++) {
			newHeaders.add(columnHeaders[i]);
		}
		
		for(String key : functionMap.keySet()) {
			Map<String, String> map = (Map)functionMap.get(key);
//			if(!ArrayUtilityMethods.arrayContainsValue(columnHeaders, key)) {
				newHeaders.add(map.get("calcName"));//columnHeaderMap.get(key);
				i++;
//			}
		}
	
		String[] newColHeaders = newHeaders.toArray(new String[newHeaders.size()]);
		
		
//		Map<String, Integer> indexMap = new HashMap<String, Integer>();
//		for(String column : columnHeaderMap.keySet()) {
//			String key = columnHeaderMap.get(column);
//			int index = ArrayUtilityMethods.arrayContainsValueAtIndex(origColHeaders, column);
//			indexMap.put(key, index);
//		}
		
		BTreeDataFrame statTable = new BTreeDataFrame(newColHeaders, newColHeaders);
		TreeNode columnRoot = table.simpleTree.nodeIndexHash.get(columnHeaders[0]);
		List<String> skipColumns = getSkipColumns(columnHeaderMap);
		
		String[] adjustedColHeaders = new String[origColHeaders.length - skipColumns.size()];
		String[] skipCol = skipColumns.toArray(new String[skipColumns.size()]);
		int b = 0;
		for(int a = 0; a < origColHeaders.length; a++) {
			if(!ArrayUtilityMethods.arrayContainsValue(skipCol, origColHeaders[a])) {
				adjustedColHeaders[b] = origColHeaders[a];
				b++;
			}
		}
		
		Map<String, Integer> indexMap = new HashMap<String, Integer>();
		for(String column : columnHeaderMap.keySet()) {
			String key = columnHeaderMap.get(column);
			int index = ArrayUtilityMethods.arrayContainsValueAtIndex(adjustedColHeaders, column);
			indexMap.put(key, index);
		}
		
		
		
		Iterator<Object[]> iterator = new MultiColumnStatIterator(columnRoot, mathModes, skipColumns, columnHeaders);
		Map<String, Object> newRow = new HashMap<String, Object>();
		while(iterator.hasNext()) {
			Object[] row = iterator.next();
//			Map<String, Object> newRow = new HashMap<String, Object>();
			for(int x = 0; x < row.length; x++) {
				newRow.put(newColHeaders[x], row[indexMap.get(newColHeaders[x])]);
			}
			statTable.addRow(newRow);
		}
		
		return statTable;
	}
	
	private Map<String, String> getNewColumnHeaders() {
		
		//Create the new column headers for the new tree
		Map<String, String> newColHeaders = new HashMap<String, String>(origColHeaders.length);
		for(String columnHeader : columnHeaders) {
			newColHeaders.put(columnHeader, columnHeader);
		}
		
		for(String key : functionMap.keySet()) {
			
			Map<String, String> map = (Map<String, String>)functionMap.get(key);
			String name = map.get("name");
			String newName = map.get("calcName");
			newColHeaders.put(name, newName);	
		}
		
		return newColHeaders;
	}
	
	private List<String> getSkipColumns(Map<String, String> columnHeaderMap) {
		
		//Don't skip columns in the function map and the group by column, skip all others
		List<String> retList = new ArrayList<String>();
		for(String column : origColHeaders) {
			if(!(columnHeaderMap.containsKey(column))) {
				retList.add(column);
			}
		}
		
		return retList;
	}
}
