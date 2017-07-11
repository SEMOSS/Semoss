package prerna.sablecc2.reactor.qs;

import prerna.query.interpreters.QueryStruct2;

public class OffsetReactor extends QueryStructReactor{

	@Override
	QueryStruct2 createQueryStruct() {
		Double l = (Double)curRow.get(0);
		Integer offset = l.intValue();
		qs.setOffSet(offset);
		return qs;
	}

}
