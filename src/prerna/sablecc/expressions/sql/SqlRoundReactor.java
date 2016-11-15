package prerna.sablecc.expressions.sql;

import java.util.Iterator;
import java.util.Map;

import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlCastSelector;
import prerna.sablecc.expressions.sql.builder.SqlRoundSelector;

public class SqlRoundReactor extends AbstractSqlExpression {

	public SqlRoundReactor() {
		setProcedureName("Round");
	}
	
	@Override
	public Iterator process() {
		super.process();
		
		// get the value at which the round should occur
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		int significantDigit = Integer.parseInt(options.get("CONDITION1") + "");
		
		IExpressionSelector previousSelector = this.builder.getLastSelector();
		
		SqlRoundSelector roundSelector = new SqlRoundSelector(previousSelector, significantDigit);
		if(significantDigit == 0) {
			SqlCastSelector castSelector = new SqlCastSelector("INT", roundSelector);
			this.builder.replaceSelector(previousSelector, castSelector);
		} else {
			this.builder.replaceSelector(previousSelector, roundSelector);
		}

		myStore.put(myStore.get(whoAmI).toString(), this.builder);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return null;
	}
}

