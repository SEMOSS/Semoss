package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class UniqueBTreeIterator implements Iterator<List<Object[]>>{

	private IndexTreeIterator iterator;
	private boolean useRawData;
	
	public UniqueBTreeIterator(TreeNode columnRoot) {
		this(columnRoot, false);
	}
	
	public UniqueBTreeIterator(TreeNode columnRoot, boolean getRawData) {
		iterator = new IndexTreeIterator(columnRoot);
		useRawData = getRawData;
	}
	
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public List<Object[]> next() {
		
		if(!this.hasNext()) {
			throw new IndexOutOfBoundsException("No more items left in interator");
		}
		
		List<Object[]> retList = new ArrayList<Object[]>();
	
		TreeNode treenode = iterator.next();	
		for(SimpleTreeNode node: treenode.instanceNode) {
			retList.addAll(getInstanceRows(node));
		}
		
		
		//TODO: if retList is empty, need to return next() instead but also need to make sure not to go out of bounds
		//This will be an issue once Filter becomes functional with the BTree
		return retList;
	}
	
	@Override
	public void remove() {
		
	}
	
	private List<Object[]> getInstanceRows(SimpleTreeNode node) {
		List<Object[]> instanceRows = new ArrayList<>();
		//populate list recursively
		getRow(instanceRows, node, true);
		return instanceRows;
	}
	
	private void getRow(List<Object[]> list, SimpleTreeNode node, boolean firstIt) {
		if(node.leftChild == null) {
			List<Object> arraylist = new ArrayList<>();
			SimpleTreeNode n = node;
			while(n!=null) {
				Object value = useRawData ? n.leaf.getRawValue() : n.leaf.getValue();
				arraylist.add(value);
				n = n.parent;
			}
			
			Collections.reverse(arraylist);
			list.add(arraylist.toArray());
			
		} else {
			getRow(list, node.leftChild, false);
		}
		
		if(node.rightSibling!=null && !firstIt) {
			getRow(list, node.rightSibling, false);
		}
	}

}
