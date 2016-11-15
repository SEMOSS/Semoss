package prerna.sablecc.expressions.sql;

import java.util.Iterator;

import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;

public class SqlSqrtReactor extends AbstractSqlExpression {

	public SqlSqrtReactor() {
		setProcedureName("Sqrt");
	}
	
	@Override
	public Iterator process() {
		super.process();

		IExpressionSelector previousSelector = this.builder.getLastSelector();
		SqlMathSelector newSelector = new SqlMathSelector(previousSelector, PKQLEnum.SQRT, "Sqrt");
		this.builder.replaceSelector(previousSelector, newSelector);

		myStore.put(myStore.get(whoAmI).toString(), this.builder);
		myStore.put("STATUS", STATUS.SUCCESS);

		return null;
	}
}