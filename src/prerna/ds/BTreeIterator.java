package prerna.ds;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class BTreeIterator implements Iterator<Object[]> {

	protected Iterator<SimpleTreeNode> iterator;
	private boolean useRawData;
	private Set<String> columns2skip;
	
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
//		useRawData = getRawData;
		this.columns2skip = columns2skip == null ? new HashSet<String>(0) : new HashSet<String>(columns2skip);
	}
	
	public BTreeIterator(TreeNode typeRoot, boolean getRawData, List<String> columns2skip, boolean getAll) {
		iterator = new ValueTreeColumnIterator(typeRoot, getAll);
		useRawData = getRawData;
		this.columns2skip = columns2skip == null ? new HashSet<String>(0) : new HashSet<String>(columns2skip);
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
		while(currValueNode != null) {
			if(!columns2skip.contains(((ISEMOSSNode)currValueNode.leaf).getType())) {
				Object value = useRawData ? currValueNode.leaf.getRawValue() : currValueNode.leaf.getValue();
				retRow.add(value);
			}
			currValueNode = currValueNode.parent;
		}
		
		//reverse the order of the list and store in an array
		Object[] nextRow = new Object[retRow.size()];
		int counter = nextRow.length-1;
		for(Object value: retRow) {
			nextRow[counter] = value;
			counter--;
		}
		
		return nextRow;
	}

	// Note: when updating to Java 8, no longer need to override remove() method
	@Override
	public void remove() {

	}
}
