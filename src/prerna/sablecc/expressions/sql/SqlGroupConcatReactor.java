package prerna.sablecc.expressions.sql;

import java.util.Map;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.ISqlSelector;
import prerna.sablecc.expressions.sql.builder.SqlBuilder;
import prerna.sablecc.expressions.sql.builder.SqlGroupConcat;

public class SqlGroupConcatReactor extends AbstractSqlBaseReducer {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlGroupConcatReactor() {
		
	}
	
	@Override
	public SqlBuilder process(H2Frame frame, SqlBuilder builder) {
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		String separator = null;
		if(options != null) {
			separator = (String) options.get("SEPARATOR");
		}
		ISqlSelector previousSelector = builder.getLastSelector();
		SqlGroupConcat newSelector = new SqlGroupConcat(previousSelector, separator);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
}
