package prerna.sablecc.expressions.sql;

import java.util.Iterator;

import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;

public class SqlAbsoluteReactor extends AbstractSqlExpression {

	@Override
	public Iterator process() {
		super.process();

		IExpressionSelector previousSelector = this.builder.getLastSelector();
		SqlMathSelector newSelector = new SqlMathSelector(previousSelector, "ABS", "Absolute");
		this.builder.replaceSelector(previousSelector, newSelector);
				
		myStore.put(myStore.get(whoAmI).toString(), this.builder);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return null;
	}
}
