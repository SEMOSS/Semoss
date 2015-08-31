package prerna.ds;

import java.util.List;

public class OrderedBTreeIterator extends BTreeIterator {

	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public OrderedBTreeIterator(List<SimpleTreeNode> orderedLeafs) {
		this(orderedLeafs, false, null);
	}
	
	public OrderedBTreeIterator(List<SimpleTreeNode> orderedLeafs, boolean getRawData) {
		this(orderedLeafs, getRawData, null);
	}
	
	public OrderedBTreeIterator(List<SimpleTreeNode> orderedLeafs, boolean getRawData, List<String> columns2skip) {
		super(null, getRawData, columns2skip);
		this.iterator = orderedLeafs.iterator();
	}
	
	public OrderedBTreeIterator(List<SimpleTreeNode> orderedLeafs, boolean getRawData, List<String> columns2skip, boolean getAll) {
		super(null, getRawData, columns2skip, getAll);
		this.iterator = orderedLeafs.iterator();
	}
}
