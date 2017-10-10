package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;

public class LimitReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		Long limit = ( (Number) curRow.get(0)).longValue();
		qs.setLimit(limit);
		return qs;
	}
}
