package prerna.sablecc.expressions.sql;

import java.util.Iterator;

import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.sql.builder.ISqlSelector;
import prerna.sablecc.expressions.sql.builder.SqlConcatSelector;

public class SqlConcatReactor extends AbstractSqlExpression {

	@Override
	public Iterator process() {
		super.process();

		int size = this.builder.selectorSize();
		ISqlSelector[] selectors = new ISqlSelector[size];
		for(int i = 0; i < size; i++) {
			selectors[i] = this.builder.getSelector(i);
		}
		
		SqlConcatSelector concatSelector = new SqlConcatSelector(selectors);
		// remove all the existing selectors 
		for(ISqlSelector selector : selectors) {
			this.builder.removeSelector(selector);
		}
		// append only the new selector to add
		this.builder.addSelector(concatSelector);
		
		myStore.put(myStore.get(whoAmI).toString(), this.builder);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		return null;
	}
}
