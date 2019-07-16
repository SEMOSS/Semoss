package prerna.sablecc.expressions.sql;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlDistinctMathSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;

public class SqlUniqueCountReactor extends AbstractH2SqlBaseReducer {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlUniqueCountReactor() {
		this.setRoutine("COUNT");
		this.setPkqlRoutine("UniqueCount");
	}
	
	public SqlExpressionBuilder process(H2Frame frame, SqlExpressionBuilder builder) {
		IExpressionSelector previousSelector = builder.getLastSelector();
		SqlDistinctMathSelector newSelector = new SqlDistinctMathSelector(previousSelector, this.routine, this.pkqlRoutine);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}


	
}
