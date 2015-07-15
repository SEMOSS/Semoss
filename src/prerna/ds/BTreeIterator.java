package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BTreeIterator implements Iterator<Object[]> {

	private Iterator<SimpleTreeNode> iterator;
	private boolean useRawData;
	private List<String> columns2skip;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public BTreeIterator(TreeNode typeRoot) {
		this(typeRoot, false, null);
	}
	
	public BTreeIterator(TreeNode typeRoot, boolean getRawData) {
		this(typeRoot, getRawData, null);
	}
	
	public BTreeIterator(TreeNode typeRoot, boolean getRawData, List<String> columns2skip) {
		iterator = new ValueTreeColumnIterator(typeRoot);
		useRawData = getRawData;
		this.columns2skip = columns2skip == null ? new ArrayList<String>() : columns2skip;
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
			if(columns2skip.contains(((ISEMOSSNode)currValueNode.leaf).getType())) {
				continue;
			} else {
				Object value = useRawData ? currValueNode.leaf.getRawValue() : currValueNode.leaf.getValue();
				retRow.add(value);
			}
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
