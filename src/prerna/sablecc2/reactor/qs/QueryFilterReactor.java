package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		GenRowStruct filters = getNounStore().getNoun("f");
		for(int i = 0; i < filters.size(); i++) {
			Filter nextFilter = (Filter)filters.get(i);
			qs.addFilter(nextFilter.getLComparison().get(0).toString(), nextFilter.getComparator(), nextFilter.getRComparison().vector);
		}
		return qs;
	}
}
