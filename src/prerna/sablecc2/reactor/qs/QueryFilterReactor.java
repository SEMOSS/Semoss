package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStruct2;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		GenRowStruct filters = getNounStore().getNoun("f");
		for(int i = 0; i < filters.size(); i++) {
			Filter nextFilter = (Filter)filters.get(i);
			qs.addFilter(nextFilter.getSelector(), nextFilter.getComparator(), nextFilter.getValues());
		}
		return qs;
	}
}
