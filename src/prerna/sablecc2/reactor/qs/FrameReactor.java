package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.QueryStruct2;

public class FrameReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		return qs;
	}
}
