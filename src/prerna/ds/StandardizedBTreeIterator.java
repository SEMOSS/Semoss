package prerna.ds;

import java.util.Iterator;
import java.util.List;

public class StandardizedBTreeIterator implements Iterator<Object[]>{

	private BTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] avg;
	private Double[] stdv;
	
	public StandardizedBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation) {
		this(typeRoot, isNum, average, standardDeviation, false, null);
	}
	
	public StandardizedBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation, boolean getRawData) {
		this(typeRoot, isNum, average, standardDeviation, getRawData, null);
	}
	
	public StandardizedBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation, boolean getRawData, List<String> columns2skip) {
		iterator = new BTreeIterator(typeRoot, getRawData, columns2skip);
		isNumeric = isNum;
		avg = average;
		stdv = standardDeviation;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return iterator.hasNext();
	}

	@Override
	public Object[] next() {
		if(this.hasNext()) {
			
			Object[] nextRow = iterator.next();
			
			for(int i = 0; i < nextRow.length; i++) {
				if(isNumeric[i]) {
					if(nextRow[i] instanceof Number) {
						nextRow[i] = ((Double)nextRow[i] - avg[i])/stdv[i];
					} else {
						nextRow[i] = null;
					}
				}
			}
			
			return nextRow;
		} else {
			throw new IndexOutOfBoundsException("No more elements in the iterator");
		}
	}
	
	@Override
	public void remove(){
		
	}
	
	
}
