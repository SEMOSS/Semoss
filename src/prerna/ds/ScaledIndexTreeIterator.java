package prerna.ds;

import java.util.Iterator;

public class ScaledIndexTreeIterator implements Iterator<Double>{

	private IndexTreeIterator iterator;
	private double min;
	private double max;
	
	ScaledIndexTreeIterator(TreeNode root, double min, double max) {
		iterator = new IndexTreeIterator(root);
		this.min = min;
		this.max = max;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return iterator.hasNext();
	}

	@Override
	public Double next() {
		TreeNode nextNode = iterator.next();
		Double value = (Double)nextNode.leaf.getValue();
		
		return (value-min)/(max-min);
	}
	
	public void remove() {

	}

}
