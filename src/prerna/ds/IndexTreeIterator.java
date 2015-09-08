package prerna.ds;

import java.util.Iterator;
import java.util.Stack;

public class IndexTreeIterator implements Iterator<TreeNode> {

	private Stack<TreeNode> nodeStack;
	private TreeNode returnNode;
	
	/**
	 * Constructor for the IndexTreeIterator
	 * 
	 * @param typeRoot			
	 */
	public IndexTreeIterator(TreeNode typeRoot) {
		nodeStack = new Stack<>();
		if(typeRoot!=null) {
			addNextNodesToStack(typeRoot.getLeft(typeRoot));
			findNextInstance();
		}
	}
	
	@Override
	/**
	 * Perform an In-Order depth-first-search (DFS)
	 */
	public boolean hasNext() {
		return (returnNode!=null);
	}
	
	@Override
	public TreeNode next() {
		if(!hasNext()) {
			throw new IndexOutOfBoundsException("No more nodes in Index Tree.");
		}
		
		TreeNode rNode = returnNode;
		returnNode = null;
		findNextInstance();
		return rNode;
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
	
	private void findNextInstance() {
		while(returnNode==null && !nodeStack.isEmpty()) {
			TreeNode nextNode = nodeStack.pop();
			
			if(nextNode.rightSibling != null) {
				addNextNodesToStack(nextNode.rightSibling);
			} else if(nextNode.rightChild != null) {
				addNextNodesToStack(nextNode.rightChild);
			}

			if(nextNode.instanceNode.size() + nextNode.filteredInstanceNode.size() > 0) {
				returnNode = nextNode;
			}
		}
	}
	
	/**
	 * Index tree's siblings share left/right children except for the most left and most right
	 * Most left has a left child that is not shared
	 * Most right has a right child that is not shared
	 */
	private void addNextNodesToStack(TreeNode parentNode) {
		nodeStack.push(parentNode);
		
		if(parentNode.leftChild != null) {
			addNextNodesToStack(parentNode.leftChild);
		}
	}
}
