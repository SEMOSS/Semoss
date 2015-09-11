package prerna.ds;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.impl.ExactStringMatcher;
import prerna.algorithm.learning.util.DuplicationReconciliation;

public class ITableStatCounter {

	private static final String AVG = "average";
	private static final String MAX = "max";
	private static final String MIN = "min";
	private static final String MEDIAN = "median";
	private static final String MODE = "mode";
	private static final String COUNT = "count";
	
//	private BTreeDataFrame table;
//	private Map<String, String> functionMap;
//	private String columnHeader;
	
	private ITableStatCounter() {

	}
	
	public static void addStatsToDataFrame(ITableDataFrame table, String columnHeader, Map<String, String> functionMap) {
//		this.columnHeader = columnHeader;
//		this.table = (BTreeDataFrame)table;
//		this.functionMap = functionMap;
		if(!(table instanceof BTreeDataFrame)) {
			throw new IllegalArgumentException("only table types of BTreeDataFrame supported");
		}
		
		DuplicationReconciliation[] duprec = getDuplicationReconciliation(functionMap);
		BTreeDataFrame newTable = createNewBTree((BTreeDataFrame)table, columnHeader, duprec, functionMap);
		table.join(newTable, columnHeader, columnHeader, 1.0, new ExactStringMatcher());
	}
	
	private static DuplicationReconciliation[] getDuplicationReconciliation(Map<String, String> functionMap) {
		DuplicationReconciliation[] array = new DuplicationReconciliation[functionMap.keySet().size()];
		int i = 0;
		for(String key : functionMap.keySet()) {
		
			DuplicationReconciliation.ReconciliationMode mode; 
			switch(key) {
				case AVG : mode = DuplicationReconciliation.ReconciliationMode.MEAN; break;
					
				case MAX : mode = DuplicationReconciliation.ReconciliationMode.MAX; break;
					
				case MIN : mode = DuplicationReconciliation.ReconciliationMode.MIN; break;
					
				case MEDIAN : mode = DuplicationReconciliation.ReconciliationMode.MEDIAN; break;
				 
				case MODE : mode = DuplicationReconciliation.ReconciliationMode.MODE; break;
				
				case COUNT : mode = DuplicationReconciliation.ReconciliationMode.COUNT; break;
				
				default : mode = DuplicationReconciliation.ReconciliationMode.MEAN; break;
			}
			array[i] = new DuplicationReconciliation(mode);
			i++;
		}
		
		return array;
	}
	
	private static BTreeDataFrame createNewBTree(BTreeDataFrame table, String columnHeader, DuplicationReconciliation[] mathModes, Map<String, String> functionMap) {
		Map<String, String> columnHeaderMap = createNewColumnHeaders(table, columnHeader, functionMap);
		String[] newColHeaders = new String[columnHeaderMap.keySet().size()];
		newColHeaders[0] = columnHeader;
		
		int i = 1;
		for(String key : columnHeaderMap.keySet()) {
			if(!key.equals(columnHeader)) {
				newColHeaders[i] = columnHeaderMap.get(key);
				i++;
			}
		}
		
		BTreeDataFrame statTable = new BTreeDataFrame(newColHeaders, newColHeaders);
		TreeNode columnRoot = table.simpleTree.nodeIndexHash.get(columnHeader);
		Iterator<Object[]> iterator = new StatIterator(columnRoot, mathModes);
		
		String[] originalColumnHeaders = table.getColumnHeaders();
		while(iterator.hasNext()) {
			Object[] row = iterator.next();
			Map<String, Object> newRow = new HashMap<String, Object>();
			for(int x = 0; x < row.length; x++) {
				String newColHeader = columnHeaderMap.get(originalColumnHeaders[i]);
				newRow.put(newColHeader, row[i]);
			}
			statTable.addRow(newRow, newRow);
		}
		return statTable;
	}
	
	private static Map<String, String> createNewColumnHeaders(BTreeDataFrame table, String columnHeader, Map<String, String> functionMap) {
		
		String[] colHeaders = table.getColumnHeaders();
		Map<String, String> newColHeaders = new HashMap<String, String>(colHeaders.length);
		newColHeaders.put(columnHeader, columnHeader);
		
		int i = 1;
		String col;
		for(String key : functionMap.keySet()) {
			if(!key.equals(columnHeader)) {
				col = key+" "+functionMap.get(key)+" on "+columnHeader;
				newColHeaders.put(key, col);
				i++;
			}
		}
		return newColHeaders;
	}
}
