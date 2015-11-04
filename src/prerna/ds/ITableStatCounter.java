package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;

public class ITableStatCounter {

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
	
	public ITableStatCounter() {
		
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
//			String c = origColHeaders[i];
			if(c.equals(columnHeader)) {
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
		
		BTreeDataFrame statTable = new BTreeDataFrame(newColHeaders, newColHeaders);
		TreeNode columnRoot = table.simpleTree.nodeIndexHash.get(columnHeader);
		List<String> skipColumns = getSkipColumns(columnHeaderMap);
		Iterator<Object[]> iterator = new StatIterator(columnRoot, mathModes, skipColumns);
		
		while(iterator.hasNext()) {
			Object[] row = iterator.next();
			Map<String, Object> newRow = new HashMap<String, Object>();
			for(int x = 0; x < row.length; x++) {
//				String newColHeader = columnHeaderMap.get(newColHeaders[i]);
				
				newRow.put(columnHeaderMap.get(tempColHeaders.get(x)), row[x]);
			}
			statTable.addRow(newRow, newRow);
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
	
//	private String verifyColumnName(String name, int count) {
//		
//		String newName;
//		if(count != 1) {
//			newName = name+"_"+count;
//		} else {
//			newName = name;
//		}
//		
//		for(String s : origColHeaders) {
//			if(s.equals(newName)) {
//				return verifyColumnName(name, ++count);
//			}
//		}
//		
//		return newName;
//	}
}
