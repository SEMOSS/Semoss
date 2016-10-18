package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.SqlExpressionIterator;
import prerna.util.Utility;

public class SqlConcatReactor extends MathReactor {
	
	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		modExpression();
		// get the sql script
		// note that the concat has 
		String script = myStore.get("MOD_" + whoAmI).toString();

		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
		// get the join columns
		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		
		// set an alias for the new column
		String aliasColumn = "CONCAT_COLUMN_" + Utility.getRandomString(7);
		
		// create the expression iterator
		SqlExpressionIterator it = new SqlExpressionIterator(frame, "CONCAT(" + script.replace("[","").replace("]", "") + ")", aliasColumn, columnsArray);

		myStore.put(nodeStr, it);
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return null;
	}
	
}
