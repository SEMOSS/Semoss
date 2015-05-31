package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BTreeIterator implements Iterator<Object[]> {

	private TreeNode currNode;
	private int index = 0;
	private List<SimpleTreeNode> instances;
	private List<TreeNode> childrenList;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public BTreeIterator(TreeNode typeRoot) {
		currNode = typeRoot;
		currNode = currNode.getLeft(currNode);
		instances = currNode.getInstances();
		
		childrenList = new ArrayList<TreeNode>();
		
		if(currNode.rightChild != null) {
			childrenList.add(0, currNode.rightChild);
		}
		if(currNode.leftChild != null) {
			childrenList.add(0, currNode.leftChild);
		}
	}
	
	@Override
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	public boolean hasNext() {
		// if more instances associated with current node still need to be 
		if(index < instances.size()) {
			return true;
		} else {
			// since we are always starting at most left, only need to check if right child exists
			if(currNode.rightSibling != null) {
				currNode = currNode.rightSibling;
				instances = currNode.getInstances();
				// got new instances list, set index back to zero
				index = 0;
				return true;
			} else {
				// grab first child
				if(!childrenList.isEmpty()) {
					currNode = childrenList.get(0);
					childrenList.remove(0);
					// since we didn't make sure we add the most left child, ensure it is here
					currNode = currNode.getLeft(currNode);
					instances = currNode.getInstances();
					// got new instances list, set index back to zero
					index = 0;
					
					// add children to children list
					if(currNode.rightChild != null) {
						childrenList.add(0, currNode.rightChild);
					}
					if(currNode.leftChild != null) {
						childrenList.add(0, currNode.leftChild);
					}
					return true;
				}
			}
		}
		
		return false;
	}
	
	@Override
	public Object[] next() {
		if(index > instances.size()) {
			throw new IndexOutOfBoundsException("No more rows in data-frame.");
		}
		
		SimpleTreeNode currValueNode = instances.get(index);
		// update index for next time method is called
		index++;
		
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
