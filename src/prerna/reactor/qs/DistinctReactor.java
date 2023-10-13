package prerna.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;

public class DistinctReactor extends AbstractQueryStructReactor {
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		boolean isDistinct = true;
		if(!this.curRow.isEmpty()) {
			isDistinct = (boolean) this.curRow.get(0);
		}
		((SelectQueryStruct) this.qs).setDistinct(isDistinct);
		return qs;
	}
	
}
