package prerna.sablecc2.reactor.qs;

import java.util.List;
import java.util.Vector;

import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.PkslDataTypes;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		List<Object> filters = getCurRow().getColumnsOfType(PkslDataTypes.FILTER);
		for(int i = 0; i < filters.size(); i++) {
			Filter nextFilter = (Filter)filters.get(i);
			qs.addFilter(nextFilter);
		}
		return qs;
	}
}
