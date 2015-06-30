package prerna.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ClusterIterator implements Iterator<List<Object[]>>{

	private IndexTreeIterator iterator;
	
	ClusterIterator(TreeNode columnRoot) {
		iterator = new IndexTreeIterator(columnRoot);
	}
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public List<Object[]> next() {
		
		List<Object[]> retList = new ArrayList<Object[]>();
	
		TreeNode treenode = iterator.next();	
		for(SimpleTreeNode node: treenode.instanceNode) {
			retList.addAll(getInstanceRows(node));
		}
		
		return retList;
	}
	
	@Override
	public void remove() {
		
	}
	
	private List<Object[]> getInstanceRows(SimpleTreeNode node) {
		List<Object[]> instanceRows = new ArrayList<>();
		//populate list recursively
		getRow(instanceRows, node);
		return instanceRows;
	}
	
	private void getRow(List<Object[]> list, SimpleTreeNode node) {
		if(node.leftChild == null) {
			List<Object> arraylist = new ArrayList<>();
			SimpleTreeNode n = node;
			while(n!=null) {
				arraylist.add(((ISEMOSSNode)n.leaf).getValue());
				n = n.parent;
			}
			
			Collections.reverse(arraylist);
			list.add(arraylist.toArray());
			
		} else {
			getRow(list, node.leftChild);
		}
		
		if(node.rightSibling!=null) {
			getRow(list, node.rightSibling);
		}
	}

}
