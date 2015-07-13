package prerna.ds;

import java.util.Iterator;

public class ScaledBTreeIterator implements Iterator<Object[]>{

	private BTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] min;
	private Double[] max;
	
	public ScaledBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max) {
		this(typeRoot, isNum, min, max, false);
	}
	
	public ScaledBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max, boolean getRawData) {
		iterator = new BTreeIterator(typeRoot, getRawData);
		isNumeric = isNum;
		this.min = min;
		this.max = max;
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
						nextRow[i] = ((Double)nextRow[i] - min[i])/(max[i] - min[i]); 
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
