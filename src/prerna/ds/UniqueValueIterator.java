package prerna.ds;

import java.util.Iterator;

public class UniqueValueIterator implements Iterator<Object> {

	Iterator<TreeNode> iterator;
	boolean getRawData;
	
	public UniqueValueIterator(TreeNode root, boolean getRawData, boolean iterateAll) {
		if(iterateAll) {
			iterator = new IndexTreeIterator(root);
		} else {
			iterator = new FilteredIndexTreeIterator(root);
		}
		
		this.getRawData = getRawData;
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return iterator.hasNext();
	}

	@Override
	public Object next() {
		if(getRawData) {
			return iterator.next().leaf.getRawValue();
		} else {
			return iterator.next().leaf.getValue();
		}
	}
	
	@Override
	public void remove() {
	}

}
