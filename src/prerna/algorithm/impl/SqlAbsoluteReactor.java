package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.SqlExpressionIterator;
import prerna.util.Utility;

public class SqlAbsoluteReactor extends MathReactor {

	@Override
	public Iterator process() {
		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
				
		// modify the expression to get the sql syntax
		modExpression();
		
		// get the value to take the absolute value of
		String script = myStore.get("MOD_" + whoAmI).toString();
		script = script.replace("[", "").replace("]", "");

		Vector<String> columns=  (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String column = columns.get(0);
		String name = "ABS_" + column + "_" + Utility.getRandomString(6);

		// sql script is ABS around the column
		String expression = "ABS(" + script + ")";
		SqlExpressionIterator it = new SqlExpressionIterator(frame, expression, name, new String[]{});
		
		String pkql = myStore.get(whoAmI).toString();
		myStore.put(pkql, it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}
