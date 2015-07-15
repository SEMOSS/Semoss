package prerna.ds;

import java.util.Iterator;
import java.util.List;

public class ScaledBTreeIterator implements Iterator<Object[]>{

	private BTreeIterator iterator;
	private boolean[] isNumeric;
	private Double[] min;
	private Double[] max;
	
	public ScaledBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max) {
		this(typeRoot, isNum, min, max, false, null);
	}
	
	public ScaledBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max, boolean getRawData) {
		this(typeRoot, isNum, min, max, getRawData, null);
	}
	
	public ScaledBTreeIterator(TreeNode typeRoot, boolean[] isNum, Double[] min, Double[] max, boolean getRawData, List<String> columns2skip) {
		iterator = new BTreeIterator(typeRoot, getRawData, columns2skip);
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
