package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class DatabaseReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		//get the selectors
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		String engineName = (String)allNouns.get(0);
		qs.setEngineName(engineName);
		return qs;
	}
}
