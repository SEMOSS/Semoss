package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class UniqueBTreeIterator implements Iterator<List<Object[]>>{

	private IndexTreeIterator iterator;
	private Queue<TreeNode> indexTreeNodes;
	private boolean useRawData;
	List<String> columns2skip;
	
	public UniqueBTreeIterator(TreeNode columnRoot) {
		this(columnRoot, false, null);
	}
	
	public UniqueBTreeIterator(TreeNode columnRoot, boolean getRawData) {
		this(columnRoot, getRawData, null);
	}
	
	public UniqueBTreeIterator(TreeNode columnRoot, boolean getRawData, List<String> columns2skip) {
		iterator = new IndexTreeIterator(columnRoot);
		useRawData = getRawData;
		indexTreeNodes = new LinkedList<TreeNode>();
		this.columns2skip = columns2skip == null ? new ArrayList<String>() : columns2skip;
		addToQueue();
	}
	
	
	@Override
	public boolean hasNext() {
		return !indexTreeNodes.isEmpty();
	}

	@Override
	public List<Object[]> next() {
		
		if(!this.hasNext()) {
			throw new IndexOutOfBoundsException("No more items left in interator");
		}
		
		List<Object[]> retList = new ArrayList<Object[]>();
	
		//TreeNode treenode = iterator.next();
		TreeNode treenode = indexTreeNodes.poll();
		for(SimpleTreeNode node: treenode.instanceNode) {
			retList.addAll(getInstanceRows(node));
		}
		
		addToQueue();
		return retList;
	}
	
	@Override
	public void remove() {
		
	}
	
	private void addToQueue() {
		while(indexTreeNodes.size() < 2 && iterator.hasNext()) {
			TreeNode nextNode = iterator.next();
			if(!nextNode.instanceNode.isEmpty()) {
				indexTreeNodes.add(nextNode);
			}
		}
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
				if(!columns2skip.contains(((ISEMOSSNode)n.leaf).getType())) {
					Object value = useRawData ? n.leaf.getRawValue() : n.leaf.getValue();
					arraylist.add(value);
				}
				n = n.parent;
			}
			
			//TODO: make this more efficient by reverse populating an array thus removing need to reverse and toArray
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
