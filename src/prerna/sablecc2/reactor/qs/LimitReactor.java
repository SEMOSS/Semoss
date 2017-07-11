package prerna.sablecc2.reactor.qs;

import prerna.query.interpreters.QueryStruct2;

public class LimitReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		Double l = (Double)curRow.get(0);
		Integer limit = l.intValue();
		qs.setLimit(limit);
		return qs;
	}
}
