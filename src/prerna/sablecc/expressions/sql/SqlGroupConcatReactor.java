package prerna.sablecc.expressions.sql;

import java.util.Map;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlGroupConcat;

public class SqlGroupConcatReactor extends AbstractH2SqlBaseReducer {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlGroupConcatReactor() {
		
	}
	
	@Override
	public SqlExpressionBuilder process(H2Frame frame, SqlExpressionBuilder builder) {
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		String separator = null;
		if(options != null) {
			separator = (String) options.get("SEPARATOR");
		}
		IExpressionSelector previousSelector = builder.getLastSelector();
		SqlGroupConcat newSelector = new SqlGroupConcat(previousSelector, separator);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
}
