package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStruct;

public class FrameReactor extends QueryStructReactor {

	@Override
	QueryStruct createQueryStruct() {
		return new QueryStruct();
	}
}
