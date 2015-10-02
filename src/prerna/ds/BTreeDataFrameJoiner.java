package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.ArrayUtilityMethods;

public class BTreeDataFrameJoiner {

	//Multi Column join for BTreeDataFrame
	//Assumptions
	//	
//	public static void join(ITableDataFrame table1, ITableDataFrame table2, Map<String, String> tableHeaders) {
//		
//		String[] table1ColumnHeaders = table1.getColumnHeaders();
//		String[] table2ColumnHeaders = table2.getColumnHeaders();
//		
//		Set<String> keySet = tableHeaders.keySet();
//		int[] table1indexes = new int[keySet.size()];
//		int[] table2indexes = new int[keySet.size()];
//		
//		int i = 0;
//		for(String key : keySet) {
//			
//			String table1Column = key;
//			String table2Column = tableHeaders.get(key);
//			
//			int table1index = ArrayUtilityMethods.arrayContainsValueAtIndex(table1ColumnHeaders, table1Column);
//			int table2index = ArrayUtilityMethods.arrayContainsValueAtIndex(table2ColumnHeaders, table2Column);
//			
//			table1indexes[i] = table1index;
//			table2indexes[i] = table2index;
//			
//			i++;
//		}
//		
//		Iterator<List<Object[]>> table1Iterator = table1.uniqueIterator(table1ColumnHeaders[0], false);
//		Iterator<List<Object[]>> table2Iterator = table2.uniqueIterator(tableHeaders.get(table1ColumnHeaders[0]), false);
//		
////		ITableDataFrame results = performMatch(table1Iterator, table2Iterator, table1indexes, table2indexes);
//		ArrayList<TreeNode> results = performMatch(table1Iterator, table2Iterator, table1indexes, table2indexes);
//		
//		return results;
//	}
	
	
	/**
	 * 
	 * @param table1
	 * @param table2
	 * @param table1ColHeaders
	 * @param table2ColHeaders
	 * 
	 * Assumptions made:
	 * 
	 */
	public static void join(BTreeDataFrame table1, BTreeDataFrame table2, String[] table1ColHeaders, String[] table2ColHeaders) {
		
		Map<String, ArrayList<SimpleTreeNode>> bucketMap = new HashMap<String, ArrayList<SimpleTreeNode>>();
		List<String> usedKeys = new ArrayList<String>();
		
		String lowestMatchLevel = table1ColHeaders[table1ColHeaders.length-1];
		TreeNode root = table1.getBuilder().nodeIndexHash.get(lowestMatchLevel);
		ValueTreeColumnIterator treeLevelIterator = new ValueTreeColumnIterator(root);
		while(treeLevelIterator.hasNext()) {
			
			SimpleTreeNode nextNode = treeLevelIterator.next();
			String key = nextNode.leaf.getValue().toString();
			
			SimpleTreeNode parentNode = nextNode.parent;
			int index = table1ColHeaders.length - 1;
			
			while(parentNode != null) {
				String type = ((ISEMOSSNode)parentNode.leaf).getType();
				if(type.equals(table1ColHeaders[index])) {
					key = key+":::"+parentNode.leaf.getValue().toString();
					index--;
				}
				
				if(index < 0) {
					break;
				}
				
				parentNode = parentNode.parent;
			}
			
			if(bucketMap.containsKey(key)) {
				bucketMap.get(key).add(nextNode);
			} else {
				ArrayList<SimpleTreeNode> newList = new ArrayList<SimpleTreeNode>();
				newList.add(nextNode);
				bucketMap.put(key, newList);
			}
		}
		
		lowestMatchLevel = table2ColHeaders[table2ColHeaders.length-1];
		root = table2.getBuilder().nodeIndexHash.get(lowestMatchLevel);
		treeLevelIterator = new ValueTreeColumnIterator(root);
		while(treeLevelIterator.hasNext()) {
			
			SimpleTreeNode nextNode = treeLevelIterator.next();
			String key = nextNode.leaf.getValue().toString();
			
			SimpleTreeNode parentNode = nextNode.parent;
			int index = table1ColHeaders.length - 1;
			
			while(parentNode != null) {
				String type = ((ISEMOSSNode)parentNode.leaf).getType();
				if(type.equals(table1ColHeaders[index])) {
					key = key+":::"+parentNode.leaf.getValue().toString();
					index--;
				}
				
				if(index < 0) {
					break;
				}
				
				parentNode = parentNode.parent;
			}
			
			if(bucketMap.containsKey(key)) {
				ArrayList<SimpleTreeNode> instanceList = bucketMap.get(key);
				
				Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
				vec.add(nextNode.leftChild);
				String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
				
				for(SimpleTreeNode instance : instanceList) {
					SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
					SimpleTreeNode.addLeafChild(instance, hookUp);
				}
				
				usedKeys.add(key);
			}
		}
		
		//add more code later for optimization
//		for(String usedKey : usedKeys) {
//			bucketMap.remove(usedKey);
//		}
		
		
		
		
	}

//	protected ArrayList<TreeNode> performMatch(Iterator<List<Object[]>> table1Iterator, Iterator<List<Object[]>> table2Iterator, int[] table1Indexes, int[] table2Indexes) {
//		
//		//Need to fix in case table is only one value in length, won't go in while loop
//		List<Object[]> table1Values = table1Iterator.next();
//		List<Object[]> table2Values = table2Iterator.next();
//		
//		int table1MainIndex = table1Indexes[0];
//		int table2MainIndex = table2Indexes[0];
//		
//		
//		while(table1Iterator.hasNext() && table2Iterator.hasNext()) {
//			
//			int comparison = this.compareTo(table1Values.get(0)[table1MainIndex].toString(), table2Values.get(0)[table2MainIndex].toString());
//
//			//if values are equal
//			if(comparison == 0) {
//					
//				Set<String> valueSet;
//				if(table1Values.size() < table2Values.size()) {
//					
//					valueSet = new HashSet<String>(table1Values.size());
//					for(int i = 0; i < table1Values.size(); i++) {
//						Object[] nextRow = table1Values.get(i);
//						
//						String row = "";
//						for(int j = 0; j < table1Indexes.length; j++) {
//							row = row + nextRow[table1Indexes[j]];
//						}
//						valueSet.add(row);
//					}
//					
//					for(int i = 0; i < table2Values.size(); i++) {
//						Object[] nextRow = table2Values.get(i);
//						String row = "";
//						for(int j = 0; j < table2Indexes.length; j++) {
//							row = row + nextRow[table2Indexes[j]];
//						}
//						
//						if(valueSet.contains(row)) {
//							//do something
//						}
//					}
//					
//				} else {
//					valueSet = new HashSet<String>(table2Values.size());
//					for(int i = 0; i < table2Values.size(); i++) {
//						
//						Object[] nextRow = table2Values.get(i);
//						
//						String row = "";
//						for(int j = 0; j < nextRow.length; j++) {
//							row = row + nextRow[table2Indexes[j]];
//						}
//						valueSet.add(row);
//					}
//					
//					for(int i = 0; i < table1Values.size(); i++) {
//						Object[] nextRow = table1Values.get(i);
//						String row = "";
//						for(int j = 0; j < nextRow.length; j++) {
//							row = row + nextRow[table1Indexes[j]];
//						}
//						
//						if(valueSet.contains(row)) {
//							//do something
//						}
//					}
//				}
//				
//			} else if(comparison == -1) {
//				table2Values = table2Iterator.next();
//			} else if(comparison == 1) {
//				table1Values = table1Iterator.next();
//			}
//		}
//		
//		
//		
//		String table1ValueKey = "Table1Value";
//		String table2ValueKey = "Table2Value";
//		
//		ITableDataFrame bTree = new BTreeDataFrame(new String[]{table1ValueKey, table2ValueKey});
//
//		//make this O(n)
//		int success = 0;
//		int total = 0;
//		for(int j = 0; j < table1Col.length; j++) {
//			if(match(table1Col[i],table2Col[j])) {
//				Map<String, Object> row = new HashMap<String, Object>();
//				row.put(table1ValueKey , table1Col[i]);
//				row.put(table2ValueKey, table2Col[j]);
//				bTree.addRow(row, row); //TODO: adding values as both raw and clean
//				success++;
//			}
//			total++;
//		}
//		
//		boolean matchNext = true;
//		while(matchNext) {
//			
//		}
//		
//		this.resultMetadata.put("success", success);
//		this.resultMetadata.put("total", total);
//		
//		return bTree;
//	}
//	
//	protected int compareTo(String obj1, String obj2) {
//		return obj1.compareTo(obj2);
//	}
}
