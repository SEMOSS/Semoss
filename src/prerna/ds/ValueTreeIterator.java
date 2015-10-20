package prerna.ds;

import java.util.Iterator;
import java.util.Stack;

public class ValueTreeIterator implements Iterator<SimpleTreeNode> {
	
	private Stack<SimpleTreeNode> nodeStack;
	private SimpleTreeNode returnNode;
	
	/**
	 * Constructor for the ValueTreeIterator
	 * 
	 * @param typeRoot
	 */
	public ValueTreeIterator(SimpleTreeNode simpleTree) {
		nodeStack = new Stack<>();
		if (simpleTree != null) {
			addNextNodesToStack(simpleTree);
			findNextInstance();
		}
	}
	
	@Override
	/**
	 * Perform an In-Order depth-first-search (DFS)
	 */
	public boolean hasNext() {
		return (returnNode != null);
	}
	
	@Override
	public SimpleTreeNode next() {
		if (!hasNext()) {
			throw new IndexOutOfBoundsException("No more nodes in Index Tree.");
		}
		
		SimpleTreeNode rNode = returnNode;
		returnNode = null;
		findNextInstance();
		return rNode;
	}
	
	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {
		
	}
	
	private void findNextInstance() {
		while (returnNode == null && !nodeStack.isEmpty()) {
			SimpleTreeNode nextNode = nodeStack.pop();
			
			if (nextNode.rightSibling != null) {
				addNextNodesToStack(nextNode.rightSibling);
			}
			
			returnNode = nextNode;
		}
	}
	
	/**
	 * Index tree's siblings share left/right children except for the most left and most right Most left has a left child that is not shared Most right has a right child that is not shared
	 */
	private void addNextNodesToStack(SimpleTreeNode parentNode) {
		nodeStack.push(parentNode);
		
		if (parentNode.leftChild != null) {
			addNextNodesToStack(parentNode.leftChild);
		}
	}
	
}
