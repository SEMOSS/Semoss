package prerna.algorithm.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.H2SqlExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SqlConcatReactor extends MathReactor {
	
	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		
		// if this is wrapping an existing expression iterator
		if(myStore.get(nodeStr) instanceof H2SqlExpressionIterator) {
			((H2SqlExpressionIterator) myStore.get(nodeStr)).close();
		}
		
		modExpression();
		// get the sql script
		// note that the concat has 
		String script = myStore.get("MOD_" + whoAmI).toString();

		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
	
		// since multiple math routines can be added together
		// need to get a unique set of values used in the join
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		Set<String> joins = new HashSet<String>();
		joins.addAll(columns);
		
		// set an alias for the new column
		String aliasColumn = "CONCAT_COLUMN_" + Utility.getRandomString(7);
		
		// create the expression iterator
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, "CONCAT(" + script.replace("[","").replace("]", "") + ")", aliasColumn, joins.toArray(new String[]{}));

		myStore.put(nodeStr, it);
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return null;
	}
	
}
