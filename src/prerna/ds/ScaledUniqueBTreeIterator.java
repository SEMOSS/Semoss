package prerna.ds;

import java.util.Iterator;
import java.util.List;

public class ScaledUniqueBTreeIterator implements Iterator<List<Object[]>> {

	private UniqueBTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] min;
	private Double[] max;
	
	public ScaledUniqueBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max) {
		this(typeRoot, isNum, min, max, false);
	}
	
	public ScaledUniqueBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max, boolean getRawData) {
		iterator = new UniqueBTreeIterator(typeRoot, getRawData);
		isNumeric = isNum;
		this.min = min;
		this.max = max;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public List<Object[]> next() {
		List<Object[]> nextItem = iterator.next();
		for(Object[] nextRow: nextItem) {
			for(int i = 0; i < nextRow.length; i++) {
				if(isNumeric[i]) {
					if(nextRow[i] instanceof Number) {
						nextRow[i] = ((Double)nextRow[i] - min[i])/(max[i] - min[i]);
					} else {
						nextRow[i] = null;
					}
				}
			}
		}
		return nextItem;
	}
	
	@Override
	public void remove() {
		
	}
}
