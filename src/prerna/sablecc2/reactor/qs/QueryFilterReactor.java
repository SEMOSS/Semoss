package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.QueryFilter;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		List<Object> filters = this.curRow.getValuesOfType(PixelDataType.FILTER);
		if(filters.isEmpty()) {
			throw new IllegalArgumentException("No filter founds to append into the query");
		}
		for(int i = 0; i < filters.size(); i++) {
			QueryFilter nextFilter = (QueryFilter)filters.get(i);
			if(nextFilter != null && isValidFilter(nextFilter)) {
				qs.addFilter(nextFilter);
			}
		}
		return qs;
	}
	
	private boolean isValidFilter(QueryFilter filter) {
		QueryFilter.FILTER_TYPE filterType = QueryFilter.determineFilterType(filter);
		if(filterType == QueryFilter.FILTER_TYPE.COL_TO_VALUES) {
			// make sure right side has values
			Object rightSide = filter.getRComparison().getValue();
			if(rightSide instanceof List) {
				return ((List) rightSide).size() > 0;
			}
		} else if(filterType == QueryFilter.FILTER_TYPE.VALUES_TO_COL) {
			// make sure left side has values
			Object leftSide = filter.getLComparison().getValue();
			if(leftSide instanceof List) {
				return ((List) leftSide).size() > 0;
			}
		}
		// meh, just return true
		return true;
	}
}
