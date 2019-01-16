package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;

public class MicrosoftSqlServerInterpreter extends SqlInterpreter {

	public MicrosoftSqlServerInterpreter() {
		
	}

	public MicrosoftSqlServerInterpreter(IEngine engine) {
		super(engine);
	}
	
	public MicrosoftSqlServerInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	@Override
	public String composeQuery() {
		if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct) ) {
			if(((SelectQueryStruct) this.qs).getLimit() > 0 || ((SelectQueryStruct) this.qs).getOffset() > 0) {
				if(((SelectQueryStruct) this.qs).getOrderBy().isEmpty()) {
					// need to add an implicit order
					IQuerySelector firstSelector = this.qs.getSelectors().get(0);
					if(firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
						((SelectQueryStruct) this.qs).addOrderBy(firstSelector.getQueryStructName(), "ASC");
					} else {
						((SelectQueryStruct) this.qs).addOrderBy(firstSelector.getAlias(), null, "ASC");
					}
				}
			}
		}
		// now just feed into the above
		return super.composeQuery();
	}
	
}
