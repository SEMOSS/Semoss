package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;

public class OffsetReactor extends QueryStructReactor{

	@Override
	QueryStruct2 createQueryStruct() {
		Long offset = ( (Number) curRow.get(0)).longValue();
		qs.setOffSet(offset);
		return qs;
	}

}
