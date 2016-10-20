package prerna.sablecc.expressions.sql;

import java.util.Iterator;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class SqlConcatReactor extends AbstractSqlExpression {

	@Override
	public Iterator process() {
		super.process();

		String aliasColumn = "CONCAT_COLUMN_" + Utility.getRandomString(7);

		// get the expression
		String expression = "CONCAT(" + this.baseScript + ")";

		H2SqlExpressionIterator it = new H2SqlExpressionIterator((H2Frame) myStore.get("G"), expression, aliasColumn, this.joinColumns);
		
		myStore.put(myStore.get(whoAmI).toString(), it);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return it;
	}
}
