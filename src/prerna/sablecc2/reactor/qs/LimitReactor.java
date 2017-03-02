package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class LimitReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		Double l = (Double)allNouns.get(0);
		Integer limit = l.intValue();
		qs.setLimit(limit);
		return qs;
	}
}
