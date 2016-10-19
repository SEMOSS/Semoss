package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.SqlExpressionIterator;
import prerna.util.Utility;

public class SqlRoundReactor extends MathReactor {

	@Override
	public Iterator process() {
		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
				
		// modify the expression to get the sql syntax
		modExpression();
		
		// get the value to take the absolute value of
		String script = myStore.get("MOD_" + whoAmI).toString();
		script = script.replace("[", "").replace("]", "");

		// get the value at which the round should occur
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int significantDigit = Integer.parseInt(options.get("CONDITION1") + "");
		
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String name = "R0UND_" + Utility.getRandomString(6);

		// sql script is round around the column with the option for significant digits
		// but in the case that the significant digits are 0, we need to cast to int
		String expression = null;
		if(significantDigit == 0) {
			expression = "CAST( ROUND(" + script + ", " + significantDigit + ") AS INT ) ";
		} else {
			expression = "ROUND(" + script + ", " + significantDigit + ")";
		}
		SqlExpressionIterator it = new SqlExpressionIterator(frame, expression, name, convertVectorToArray(columns));
		
		String pkql = myStore.get(whoAmI).toString();
		myStore.put(pkql, it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}
