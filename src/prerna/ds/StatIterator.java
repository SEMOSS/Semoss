package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.learning.util.DuplicationReconciliation;

//TODO : make this more efficient by intelligently using counts
public class StatIterator implements Iterator<Object[]>{
	Iterator<List<Object[]>> iterator;
	DuplicationReconciliation[] statArray;
	
	//DuplicationReconciliation.ReconciliationMode[] reconMode;
	
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

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Object[] next() {
		if(!hasNext()) {
			throw new IndexOutOfBoundsException("No more rows in the table");
		}
		
		List<Object[]> nextData = iterator.next();
		int count = nextData.size();
		
		Object[] returnRow = new Object[nextData.get(0).length];
		
		for(Object[] array : nextData) {
			for(int i = 0; i < array.length; i++) {
				statArray[i].addValue(array[i]);
			}
		}
		
		for(int i = 0; i < returnRow.length; i++) {
//			if(reconMode[i] == DuplicationReconciliation.ReconciliationMode.COUNT) {
//				returnRow[i] = count;
//			} else {
				returnRow[i] = statArray[i].getReconciliatedValue();
				statArray[i].clearValue();
//			}
		}
		
		return returnRow;
	}
	
	@Override
	public void remove() {
		
	}
	
}
