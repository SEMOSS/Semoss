package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import com.hp.hpl.jena.reasoner.rulesys.builtins.Table;

import prerna.algorithm.learning.util.DuplicationReconciliation;

//TODO : make this more efficient by intelligently using counts
public class StatIterator implements Iterator<Object[]>{
	Iterator<List<Object[]>> iterator;
	DuplicationReconciliation[] statArray;
	int[] indexes;
	Queue<Object[]> returnData;
	
	public StatIterator(TreeNode root, DuplicationReconciliation[] stats) {
		iterator = new UniqueBTreeIterator(root);
		statArray = stats;
		//reconMode = compressionMode;
//		for(int i = 0; i < compressionMode.length; i++) {
//			//Need to prevent wasting of space
//			//if(!(compressionMode[i]==DuplicationReconciliation.ReconciliationMode.COUNT)) {
//			compressionArray[i] = new DuplicationReconciliation(compressionMode[i]);
//			//}
//		}
	}
	
	public StatIterator(TreeNode root, DuplicationReconciliation[] stats, List<String> skipColumns) {
		iterator = new UniqueBTreeIterator(root, false, skipColumns);
		statArray = stats;
	}
	
	public StatIterator(TreeNode root, DuplicationReconciliation[] stats, List<String> skipColumns, String[] columnHeaders) {
		iterator = new UniqueBTreeIterator(root, false, skipColumns);
		statArray = stats;
		getColumnHeaderIndexes(stats, columnHeaders.length);
		returnData = new LinkedList<Object[]>();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext() || !returnData.isEmpty();
	}

	@Override
	public Object[] next() {
		if(!hasNext()) {
			throw new IndexOutOfBoundsException("No more rows in the table");
		}
		
		if(!returnData.isEmpty()) {
			return returnData.poll();
		} else {
		
			List<Object[]> nextData = iterator.next();
			int count = nextData.size();
			
			int length = nextData.get(0).length;
			
			//bucketing the list of object arrays
			Map<String, List<Object[]>> bucketMap = new HashMap<String, List<Object[]>>();
			for(Object[] array : nextData) {
				String key = "";
				for(int i = 0; i < indexes.length; i++) {
					key = key+array[indexes[i]];
				}
				
				if(bucketMap.containsKey(key)) {
					bucketMap.get(key).add(array);
				} else {
					List<Object[]> alist = new ArrayList<Object[]>();
					alist.add(array);
					bucketMap.put(key, alist);
				}
			}
			
			
			for(String key : bucketMap.keySet()) {
				
				Object[] returnRow = new Object[length];
				
				List<Object[]> bucketData = bucketMap.get(key);
				for(Object[] array : bucketData) {
					for(int i = 0; i < array.length; i++) {
						if(statArray[i]==null) {
							returnRow[i] = array[i];
						} else {
							statArray[i].addValue(array[i]);
						}
					}
				}
			
			
	
				for(int i = 0; i < returnRow.length; i++) {
					if(statArray[i] != null) {
						returnRow[i] = statArray[i].getReconciliatedValue();
						statArray[i].clearValue();
					}
				}
				
				returnData.add(returnRow);
			}
			
			return returnData.poll();
		}
	}
	
	@Override
	public void remove() {
		
	}
	
	private int[] getColumnHeaderIndexes(DuplicationReconciliation[] stats, int length) {
		int[] indexes = new int[length];
		int x = 0;
		for(int i = 0; i < stats.length; i++) {
			if(stats[i] == null) {
				indexes[x] = i;
				x++;
			}
			
			if(x==length) {
				break;
			}
		}
		
		return indexes;
	}
}
