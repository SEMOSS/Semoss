package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;

import prerna.algorithm.api.IColumnOperator;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;

public class AddColumnOperator implements IColumnOperator {
	Iterator it;
	TinkerFrame frame;
	String newCol;
	String[] headersUsed;

	public AddColumnOperator(TinkerFrame frame, Iterator it, String newCol, String[] headersUsed) {
		this.it = it;
		this.frame = frame;
		this.newCol = newCol;
		this.headersUsed = headersUsed;
	}
	
	public void apply() {
		for (int i = 0; i < headersUsed.length; i++) {
			frame.connectTypes(headersUsed[i], newCol);
		}
		
		while(it.hasNext()) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			row.put(newCol, it.next());
			for(int i = 0; i < headersUsed.length; i++) {
				if (it instanceof ExpressionIterator) {
					row.put(headersUsed[i], ((ExpressionIterator)it).getOtherBindings().get(headersUsed[i]));
				}
			}
			frame.addRelationship(row, row);
		}
	}
	
}
