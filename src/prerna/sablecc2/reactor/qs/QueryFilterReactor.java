package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;

public class QueryFilterReactor extends QueryStructReactor {

	QueryStruct2 createQueryStruct() {
		List<Object> filters = this.curRow.getValuesOfType(PixelDataType.FILTER);
		if(filters.isEmpty()) {
			throw new IllegalArgumentException("No filter founds to append into the query");
		}
		for(int i = 0; i < filters.size(); i++) {
			IQueryFilter nextFilter = (IQueryFilter)filters.get(i);
			if(nextFilter != null) {
				if(nextFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
					if(isValidFilter((SimpleQueryFilter) nextFilter)) {
						qs.addFilter(nextFilter);
					}
				} else {
					qs.addFilter(nextFilter);
				}
			}
		}
		return qs;
	}
	
	private boolean isValidFilter(SimpleQueryFilter filter) {
		SimpleQueryFilter.FILTER_TYPE filterType = SimpleQueryFilter.determineFilterType(filter);
		if(filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
			// make sure right side has values
			Object rightSide = filter.getRComparison().getValue();
			if(rightSide instanceof List) {
				return ((List) rightSide).size() > 0;
			}
		} else if(filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
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
