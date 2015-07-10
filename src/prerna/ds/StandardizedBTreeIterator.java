package prerna.ds;

import java.util.Iterator;

public class StandardizedBTreeIterator implements Iterator<Object[]>{

	private BTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] avg;
	private Double[] stdv;
	
	public StandardizedBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] average, Double[] standardDeviation) {
		iterator = new BTreeIterator(typeRoot);
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
					nextRow[i] = ((Double)nextRow[i] - avg[i])/stdv[i];
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
