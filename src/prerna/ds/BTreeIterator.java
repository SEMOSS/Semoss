package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BTreeIterator implements Iterator<Object[]>{

	private List<SimpleTreeNode> leavesVec;
	private int index = 0;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param leavesVec			A list of nodes corresponding to the leaves in the tree
	 */
	public BTreeIterator(List<SimpleTreeNode> leavesVec) {
		this.leavesVec = leavesVec;
	}
	
	@Override
	public boolean hasNext() {
		if(index >= leavesVec.size()) {
			return false;
		}
		return true;
	}

	@Override
	public Object[] next() {
		SimpleTreeNode leaf = leavesVec.get(index);
		// add values into leaf from leaf to parent
		List<Object> retRow = new ArrayList<Object>();
		retRow.add(leaf.leaf.getValue());
		while(leaf.parent != null) {
			leaf = leaf.parent;
			retRow.add(leaf.leaf.getValue());
		}
		
		// reverse the values to be from parent to leaf
		Collections.reverse(retRow);
		// update index for next time method is called
		index++;
		
		return retRow.toArray();
	}

}
