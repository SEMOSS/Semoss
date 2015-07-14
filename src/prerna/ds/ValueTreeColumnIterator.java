package prerna.ds;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class ValueTreeColumnIterator implements Iterator<SimpleTreeNode>{
	private Queue<SimpleTreeNode> instances;
	private Iterator<TreeNode> iterator;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param rootLevel			A list of nodes corresponding to the leaves in the tree
	 */
	public ValueTreeColumnIterator(TreeNode rootLevel) {
		iterator = new FilteredIndexTreeIterator(rootLevel);
		instances = new LinkedList<>();
		addInstances();
	}
	
	@Override
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	public boolean hasNext() {
		return (!instances.isEmpty());
	}
	
	@Override
	public SimpleTreeNode next() {
		
		if(!instances.isEmpty()) {
			SimpleTreeNode returnNode = instances.remove();
			addInstances();
			return returnNode;
		}
		else {
			throw new IndexOutOfBoundsException("No more values in the column");
		}
	}
	
	private void addInstances() {
		while(instances.size() < 2 && iterator.hasNext()) {
			instances.addAll(iterator.next().instanceNode);
		}
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
}
