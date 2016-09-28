package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;

public class TableStatCounter {

	/*
	 * 
	 */
	private static final String AVG = "average";
	private static final String MAX = "max";
	private static final String MIN = "min";
	private static final String MEDIAN = "median";
	private static final String MODE = "mode";
	private static final String COUNT = "count";
	
	private BTreeDataFrame table;
	private Map<String, Object> functionMap;
	private String[] origColHeaders;
	private String columnHeader;
	
	public TableStatCounter() {
		
	}
	
	private void setVariables(ITableDataFrame table, String columnHeader, Map<String, Object> functionMap) {
		if(!(table instanceof BTreeDataFrame)) {
			throw new IllegalArgumentException("only table types of BTreeDataFrame supported");
		}
		
		this.columnHeader = columnHeader;
		this.table = (BTreeDataFrame)table;
		this.functionMap = functionMap;
		this.origColHeaders = table.getColumnHeaders();
	}
	
	public void addStatsToDataFrame(ITableDataFrame table, String columnHeader, Map<String, Object> functionMap) {
		setVariables(table, columnHeader, functionMap);
		
		
		DuplicationReconciliation[] duprec = getDuplicationReconciliation();
		Map<String, String> columnHeaderMap = getNewColumnHeaders();
		BTreeDataFrame newTable = createNewBTree(columnHeaderMap, duprec);
		this.table.join(newTable, columnHeader, columnHeader, 1.0, new ExactStringMatcher());

	}
	
	/**
	 */
	private DuplicationReconciliation[] getDuplicationReconciliation() {
		
//		DuplicationReconciliation[] array = new DuplicationReconciliation[functionMap.keySet().size()+1];
		List<DuplicationReconciliation> DRarray = new ArrayList<DuplicationReconciliation>();
		
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
//			String c = origColHeaders[i];
			if(c.equals(columnHeader)) {
				DRarray.add(null);
			} else {
				DuplicationReconciliation dr = duprecMap.get(c);
				if(dr != null) {
					DRarray.add(duprecMap.get(c));
				}
			}
		}
		return DRarray.toArray(new DuplicationReconciliation[DRarray.size()]);
	}
	
	// creates new BTreeDataFrame with the join columns and new columns
	private BTreeDataFrame createNewBTree(Map<String, String> columnHeaderMap, DuplicationReconciliation[] mathModes) {
		String[] newColHeaders = new String[columnHeaderMap.keySet().size()];
		List<String> tempColHeaders = new ArrayList<String>(newColHeaders.length);
		for(String c : origColHeaders) {
			if(columnHeaderMap.containsKey(c)) {
				tempColHeaders.add(c);
			}
		}
		newColHeaders[0] = columnHeader;
		
		int i = 1;
		for(String key : columnHeaderMap.keySet()) {
			if(!key.equals(columnHeader)) {
				newColHeaders[i] = columnHeaderMap.get(key);
				i++;
			}
		}
		
		//instantiate the new table that will be joined with the old table
		BTreeDataFrame statTable = new BTreeDataFrame(newColHeaders, newColHeaders);
		TreeNode columnRoot = table.simpleTree.nodeIndexHash.get(columnHeader);
		
		//gather the columns to skip, we only want to operate on the columns of interest
		List<String> skipColumns = getSkipColumns(columnHeaderMap);
		
		//stat iterator requires a column root, list of DuplicationReconciliation objects that correspond to how the values will be grouped (avg, median, etc.), and skip columns
		Iterator<Object[]> iterator = new StatIterator(columnRoot, mathModes, skipColumns);
		
		//for all values StatIterator returns, add it to the BTreeDataFrame
		while(iterator.hasNext()) {
			Object[] row = iterator.next();
			Map<String, Object> newRow = new HashMap<String, Object>();
			for(int x = 0; x < row.length; x++) {
//				String newColHeader = columnHeaderMap.get(newColHeaders[i]);
				
				newRow.put(columnHeaderMap.get(tempColHeaders.get(x)), row[x]);
			}
			statTable.addRow(newRow);
		}
		return statTable;
	}
	
	//Generalize this for multiple columns
	private Map<String, String> getNewColumnHeaders() {
		
		//Create the new column headers for the new tree
		Map<String, String> newColHeaders = new HashMap<String, String>(origColHeaders.length);
		newColHeaders.put(columnHeader, columnHeader);
		
		for(String key : functionMap.keySet()) {
			
			Map<String, String> map = (Map<String, String>)functionMap.get(key);
			String name = map.get("name");
			String newName = map.get("calcName");
			if(!name.equals(columnHeader)) {
				newColHeaders.put(name, newName);
			}
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
