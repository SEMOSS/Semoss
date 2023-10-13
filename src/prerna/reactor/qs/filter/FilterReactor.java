package prerna.reactor.qs.filter;

import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class FilterReactor extends AbstractQueryStructReactor {
	
	public FilterReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILTERS.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		List<Object> filters = this.curRow.getValuesOfType(PixelDataType.FILTER);
		for(int i = 0; i < filters.size(); i++) {
			IQueryFilter nextFilter = (IQueryFilter)filters.get(i);
			if(nextFilter != null) {
				if(nextFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
					if(isValidFilter((SimpleQueryFilter) nextFilter)) {
						qs.addExplicitFilter(nextFilter);
					}
				} else {
					qs.addExplicitFilter(nextFilter);
				}
			}
		}
		return qs;
	}
	
	protected boolean isValidFilter(SimpleQueryFilter filter) {
		SimpleQueryFilter.FILTER_TYPE filterType = filter.getSimpleFilterType();
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
