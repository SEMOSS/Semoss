package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;

public class MicrosoftSqlServerQueryInterpreter extends SqlInterpreter {

	public MicrosoftSqlServerQueryInterpreter() {
		
	}

	public MicrosoftSqlServerQueryInterpreter(IEngine engine) {
		super(engine);
	}
	
	public MicrosoftSqlServerQueryInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	@Override
	public String composeQuery() {
		if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct) ) {
			if(this.qs.getLimit() > 0 || this.qs.getOffset() > 0) {
				if(this.qs.getOrderBy().isEmpty()) {
					// need to add an implicit order
					IQuerySelector firstSelector = this.qs.getSelectors().get(0);
					if(firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
						this.qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
					} else {
						this.qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
					}
				}
			}
		}
		// now just feed into the above
		return super.composeQuery();
	}
	
}
