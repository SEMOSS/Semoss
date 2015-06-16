package prerna.ds;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Queue;

public class ValueTreeColumnIterator implements Iterator<SimpleTreeNode>{
	private Queue<SimpleTreeNode> instances;
	private IndexTreeIterator iterator;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public ValueTreeColumnIterator(TreeNode typeRoot) {
		iterator = new IndexTreeIterator(typeRoot);
		instances = new LinkedList<>();
	}
	
	@Override
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	public boolean hasNext() {
		return (!instances.isEmpty() || iterator.hasNext());
	}
	
	@Override
	public SimpleTreeNode next() {
		
		
		if(!instances.isEmpty()) {
			return instances.remove();
		}
		else {
			if(!iterator.hasNext()) {
				throw new IndexOutOfBoundsException("No more nodes in the Column.");
			}
			instances.addAll(iterator.next().instanceNode); 
			return instances.remove();
		}
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
}
