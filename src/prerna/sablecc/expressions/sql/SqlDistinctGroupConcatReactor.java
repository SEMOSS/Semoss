package prerna.sablecc.expressions.sql;

import java.util.Map;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlDistinctGroupConcat;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;

public class SqlDistinctGroupConcatReactor extends AbstractH2SqlBaseReducer {

	public SqlDistinctGroupConcatReactor() {
		this.setRoutine("UniqueGroupConcat");
		this.setPkqlRoutine("UniqueGroupConcat");
	}
	
	@Override
	public SqlExpressionBuilder process(H2Frame frame, SqlExpressionBuilder builder) {
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		String separator = null;
		if(options != null) {
			separator = (String) options.get("SEPARATOR");
		}
		IExpressionSelector previousSelector = builder.getLastSelector();
		SqlDistinctGroupConcat newSelector = new SqlDistinctGroupConcat(previousSelector, separator);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
	
}
