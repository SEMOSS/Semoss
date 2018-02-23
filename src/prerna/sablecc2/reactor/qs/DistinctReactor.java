package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;

public class DistinctReactor extends AbstractQueryStructReactor {
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		boolean isDistinct = true;
		if(!this.curRow.isEmpty()) {
			isDistinct = (boolean) this.curRow.get(0);
		}
		this.qs.setDistinct(isDistinct);
		return qs;
	}
	
}
