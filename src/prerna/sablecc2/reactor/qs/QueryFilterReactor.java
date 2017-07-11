package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.query.interpreters.QueryStruct2;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.QueryFilter;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		List<Object> filters = this.curRow.getColumnsOfType(PkslDataTypes.FILTER);
		for(int i = 0; i < filters.size(); i++) {
			QueryFilter nextFilter = (QueryFilter)filters.get(i);
			qs.addFilter(nextFilter);
		}
		return qs;
	}
}
