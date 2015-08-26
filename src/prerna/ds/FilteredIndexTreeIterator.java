package prerna.ds;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FilteredIndexTreeIterator implements Iterator<TreeNode>{

	private Queue<TreeNode> treeNodes;
	private Iterator<TreeNode> indexTreeIterator;
	
	/**
	 * Constructor for the IndexTreeIterator
	 * 
	 * @param typeRoot			
	 */
	public FilteredIndexTreeIterator(TreeNode typeRoot) {
		indexTreeIterator = new IndexTreeIterator(typeRoot);
		treeNodes = new LinkedList<TreeNode>();
		addTreeNodes();
	}
	
	@Override
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 */
	public boolean hasNext() {
		return !treeNodes.isEmpty();
	}
	
	@Override
	public TreeNode next() {
		if(!hasNext()) {
			throw new IndexOutOfBoundsException("No more nodes in Index Tree");
		}
		
		TreeNode returnNode = treeNodes.remove();
		addTreeNodes();
		return returnNode;
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
	
	private void addTreeNodes() {
		while(treeNodes.size() < 2 && indexTreeIterator.hasNext()) {
			TreeNode nextNode = indexTreeIterator.next();
			if(nextNode.instanceNode.size() > 0) {
				treeNodes.add(nextNode);
			}
		}
	}
}
