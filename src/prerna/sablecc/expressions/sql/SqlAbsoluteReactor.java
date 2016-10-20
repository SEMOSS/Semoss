package prerna.sablecc.expressions.sql;

import java.util.Iterator;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.H2SqlExpressionIterator;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SqlAbsoluteReactor extends AbstractSqlExpression {

	@Override
	public Iterator process() {
		super.process();

		String aliasColumn = "ABS_" + Utility.getRandomString(6);

		// get the expression
		String expression = "ABS(" + this.baseScript + ")";
		
		H2SqlExpressionIterator it = new H2SqlExpressionIterator((H2Frame) myStore.get("G"), expression, aliasColumn, this.joinColumns);
		
		myStore.put(myStore.get(whoAmI).toString(), it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}
