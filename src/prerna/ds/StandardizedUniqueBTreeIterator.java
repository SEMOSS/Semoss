package prerna.ds;

import java.util.Iterator;
import java.util.List;

public class StandardizedUniqueBTreeIterator implements Iterator<List<Object[]>> {

	private UniqueBTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] avg;
	private Double[] stdv;
	
	public StandardizedUniqueBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation) {
		this(typeRoot, isNum, average, standardDeviation, false, null);
	}
	
	public StandardizedUniqueBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation, boolean getRawData) {
		this(typeRoot, isNum, average, standardDeviation, getRawData, null);
	}
	
	public StandardizedUniqueBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation, boolean getRawData, List<String> columns2skip) {
		iterator = new UniqueBTreeIterator(typeRoot, getRawData, columns2skip);
		isNumeric = isNum;
		avg = average;
		stdv = standardDeviation;
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
						nextRow[i] = ((Double)nextRow[i] - avg[i])/stdv[i];
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
