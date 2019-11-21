package prerna.query.interpreters;

import prerna.query.querystruct.HardSelectQueryStruct;

public class CypherInterpreter extends AbstractQueryInterpreter {

	@Override
	public String composeQuery() {
		if (this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct) this.qs).getQuery();
		}
		
		//TODO build this out
		this.qs.getSelectors();
		
		return null;
	}

}
