package prerna.ds;

import java.util.Iterator;

public class StandardizedIndexTreeIterator implements Iterator<Double>{

	private IndexTreeIterator iterator;
	private double avg;
	private double stdv;
	
	StandardizedIndexTreeIterator(TreeNode root, double average, double standardDeviation){
		iterator = new IndexTreeIterator(root);
		avg = average;
		stdv = standardDeviation;
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
		return (value-avg)/stdv;
	}
	
	public void remove() {

	}
}
