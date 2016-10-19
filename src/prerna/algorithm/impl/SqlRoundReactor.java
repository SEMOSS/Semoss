package prerna.algorithm.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.H2SqlExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SqlRoundReactor extends MathReactor {

	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();

		// if this is wrapping an existing expression iterator
		if(myStore.get(nodeStr) instanceof H2SqlExpressionIterator) {
			((H2SqlExpressionIterator) myStore.get(nodeStr)).close();
		}
		
		// modify the expression to get the sql syntax
		modExpression();
		
		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
		
		// get the value to take the absolute value of
		String script = myStore.get("MOD_" + whoAmI).toString();
		script = script.replace("[", "").replace("]", "");

		// get the value at which the round should occur
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int significantDigit = Integer.parseInt(options.get("CONDITION1") + "");
		
		// since multiple math routines can be added together
		// need to get a unique set of values used in the join
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		Set<String> joins = new HashSet<String>();
		joins.addAll(columns);
		
		String name = "R0UND_" + Utility.getRandomString(6);

		// sql script is round around the column with the option for significant digits
		// but in the case that the significant digits are 0, we need to cast to int
		String expression = null;
		if(significantDigit == 0) {
			expression = "CAST( ROUND(" + script + ", " + significantDigit + ") AS INT ) ";
		} else {
			expression = "ROUND(" + script + ", " + significantDigit + ")";
		}
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, expression, name, joins.toArray(new String[]{}));
		
		myStore.put(nodeStr, it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}
