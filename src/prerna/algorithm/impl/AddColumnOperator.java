package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IColumnOperator;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;

@Deprecated
public class AddColumnOperator implements IColumnOperator {
	
	private static final Logger LOGGER = LogManager.getLogger(AddColumnOperator.class.getName());
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
		
		long startTime = System.currentTimeMillis();
		
//		for (int i = 0; i < headersUsed.length; i++) {
			frame.connectTypes(headersUsed[0], newCol, null);
//		}
		
		
		while(it.hasNext()) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			row.put(newCol, it.next());
			for(int i = 0; i < headersUsed.length; i++) {
				if (it instanceof ExpressionIterator) {
					row.put(headersUsed[i], ((ExpressionIterator)it).getOtherBindings().get(headersUsed[i]));
				}
			}
			frame.addRelationship(row);
		}
		
		LOGGER.info("Added column '"+newCol+"': "+(System.currentTimeMillis() - startTime)+" ms");
	}
	
}
