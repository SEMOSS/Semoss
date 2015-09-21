package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class WebBTreeIterator implements Iterator<List<HashMap<String, Object>>> {

	private Iterator<TreeNode> iterator;
	private Queue<TreeNode> indexTreeNodes;
	private boolean useRawData;
	List<String> columns2skip;
	
	public WebBTreeIterator(TreeNode columnRoot) {
		this(columnRoot, "", false, null);
	}
	
	public WebBTreeIterator(TreeNode columnRoot, boolean getRawData) {
		this(columnRoot, "", getRawData, null);
	}
	
	public WebBTreeIterator(TreeNode columnRoot, String sort, boolean getRawData, List<String> columns2skip) {

		if(sort.equals("desc")) {
			iterator = new ReverseIndexTreeIterator(columnRoot);
		} else {
			iterator = new IndexTreeIterator(columnRoot);
		}
		
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
	public List<HashMap<String, Object>> next() {
		
		if(!this.hasNext()) {
			throw new IndexOutOfBoundsException("No more items left in interator");
		}
		
		List<HashMap<String, Object>> retList = new ArrayList<HashMap<String, Object>>();
	
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
	
	private List<HashMap<String, Object>> getInstanceRows(SimpleTreeNode node) {
		List<HashMap<String, Object>> instanceRows = new ArrayList<>();
		//populate list recursively
		getRow(instanceRows, node, true);
		return instanceRows;
	}
	
	private void getRow(List<HashMap<String, Object>> list, SimpleTreeNode node, boolean firstIt) {
		
		if(node.leftChild == null) {
			HashMap<String, Object> rowMap = new HashMap<>();
			SimpleTreeNode n = node;
			while(n!=null) {
				String type = ((ISEMOSSNode)n.leaf).getType();
				//TODO : make this check more efficient
				if(!columns2skip.contains(type)) {
					Object value = useRawData ? n.leaf.getRawValue() : n.leaf.getValue();
					rowMap.put(type, value);
				}
				n = n.parent;
			}

			list.add(rowMap);
			
		} else {
			getRow(list, node.leftChild, false);
		}
		
		if(node.rightSibling!=null && !firstIt) {
			getRow(list, node.rightSibling, false);
		}
	}

}
