package prerna.ds;

import java.util.Iterator;
import java.util.Stack;

public class IndexTreeIterator implements Iterator<TreeNode> {

	private Stack<TreeNode> nodeStack;
	
	/**
	 * Constructor for the IndexTreeIterator
	 * 
	 * @param typeRoot			
	 */
	public IndexTreeIterator(TreeNode typeRoot) {
		nodeStack.push(typeRoot.getLeft(typeRoot));
	}
	
	@Override
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 */
	public boolean hasNext() {
		return !nodeStack.isEmpty();
	}
	
	@Override
	public TreeNode next() {
		if(nodeStack.isEmpty()) {
			throw new IndexOutOfBoundsException("No more nodes in Index Tree.");
		}
		TreeNode returnNode = nodeStack.pop();
		addNextNodesToStack(returnNode);
		return returnNode;
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
	
	/**
	 * Index tree's siblings share left/right children except for the most left and most right
	 * Most left has a left child that is not shared
	 * Most right has a right child that is not shared
	 * Using logic to only add left child and when the node does not have a right-sibling to add the right child
	 */
	private void addNextNodesToStack(TreeNode parentNode) {
		
		//add the right sibling if there is one
		if (parentNode.rightSibling != null) {
			nodeStack.push(parentNode.rightSibling);
		}
		
		// if this is the most right node, add the right child 
		else if(parentNode.rightSibling == null && parentNode.rightChild != null) {
			nodeStack.push(parentNode.rightChild);
		}
		
		// only add left children to children list to not double count
		if(parentNode.leftChild != null) {
			nodeStack.push(parentNode.leftChild);
		}
	}
}
