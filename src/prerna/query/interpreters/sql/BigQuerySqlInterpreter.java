package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.query.querystruct.selectors.IQuerySelector;

public class BigQuerySqlInterpreter extends SqlInterpreter {

	public BigQuerySqlInterpreter() {

	}

	public BigQuerySqlInterpreter(IDatabase engine) {
		super(engine);
	}

	public BigQuerySqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	@Override
	public void addSelector(IQuerySelector selector) {
		String alias = selector.getAlias();
		String newSelector = processSelector(selector, true) + " AS " + alias;
		if(selectors.length() == 0) {
			selectors = newSelector;
		} else {
			selectors += " , " + newSelector;
		}
		selectorList.add(newSelector);
		selectorAliases.add(alias);
	}
	
	@Override
	protected void addOrderBySelector() {
		int counter = 0;
		for(StringBuilder orderBySelector : this.orderBySelectors) {
			String alias = "o"+counter++;
			String newSelector = "("+orderBySelector+") AS " + alias;
			if(selectors.length() == 0) {
				selectors = newSelector;
			} else {
				selectors += " , " + newSelector;
			}
			selectorList.add(newSelector);
			selectorAliases.add(alias);
		}
	}

}
