package prerna.sablecc.expressions.sql;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlBuilder;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;

public abstract class H2SqlBasicMathReactor extends AbstractSqlBaseReducer {

	public SqlBuilder process(H2Frame frame, SqlBuilder builder) {
		IExpressionSelector previousSelector = builder.getLastSelector();
		SqlMathSelector newSelector = new SqlMathSelector(previousSelector, this.mathRoutine, this.pkqlMathRoutine);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
	
}