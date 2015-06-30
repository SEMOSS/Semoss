package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BTreeIterator implements Iterator<Object[]> {

	private ValueTreeColumnIterator iterator;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public BTreeIterator(TreeNode typeRoot) {
		//should default this to be the leaf type (last level)
		iterator = new ValueTreeColumnIterator(typeRoot);
	}
	
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}
	
	@Override
	public Object[] next() {
		
		SimpleTreeNode currValueNode = iterator.next();
		
		// add values into leaf from leaf to parent
		List<Object> retRow = new ArrayList<Object>();
		retRow.add(currValueNode.leaf.getValue());
		while(currValueNode.parent != null) {
			currValueNode = currValueNode.parent;
			retRow.add(currValueNode.leaf.getValue());
		}
		
		// reverse the values to be from parent to leaf
		Collections.reverse(retRow);
		
		return retRow.toArray();
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
}
