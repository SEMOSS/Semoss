package prerna.sablecc.expressions.sql;

import java.util.Iterator;
import java.util.Map;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.H2SqlExpressionIterator;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SqlRoundReactor extends AbstractSqlExpression {

	@Override
	public Iterator process() {
		super.process();

		String aliasColumn = "R0UND_" + Utility.getRandomString(6);

		// get the value at which the round should occur
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int significantDigit = Integer.parseInt(options.get("CONDITION1") + "");

		// get the expression
		String expression = null;
		if(significantDigit == 0) {
			expression = "CAST( ROUND(" + this.baseScript + ", " + significantDigit + ") AS INT ) ";
		} else {
			expression = "ROUND(" + this.baseScript + ", " + significantDigit + ")";
		}

		H2SqlExpressionIterator it = new H2SqlExpressionIterator((H2Frame) myStore.get("G"), expression, aliasColumn, this.joinColumns);
		
		myStore.put(myStore.get(whoAmI).toString(), it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}

