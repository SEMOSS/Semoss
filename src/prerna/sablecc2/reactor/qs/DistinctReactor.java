package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.SelectQueryStruct;

public class DistinctReactor extends AbstractQueryStructReactor {
	
	@Override
	protected SelectQueryStruct createQueryStruct() {
		boolean isDistinct = true;
		if(!this.curRow.isEmpty()) {
			isDistinct = (boolean) this.curRow.get(0);
		}
		this.qs.setDistinct(isDistinct);
		return qs;
	}
	
}
