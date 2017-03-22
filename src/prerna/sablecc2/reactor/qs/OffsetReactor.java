package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class OffsetReactor extends QueryStructReactor{

	@Override
	QueryStruct2 createQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		Double l = (Double)allNouns.get(0);
		Integer offset = l.intValue();
		qs.setOffSet(offset);
		return qs;
	}

}
