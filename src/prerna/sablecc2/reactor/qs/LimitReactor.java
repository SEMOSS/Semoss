package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.ReactorKeysEnum;

public class LimitReactor extends AbstractQueryStructReactor {
	
	public LimitReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.NUMERIC_VALUE.getKey()};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
		if(curRow.get(0) instanceof Number) {
			Long limit = ( (Number) curRow.get(0)).longValue();
			qs.setLimit(limit);
		}
		return qs;
	}
}
