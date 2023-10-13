package prerna.reactor.qs.filter;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter.QUERY_FILTER_TYPE;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryFilterComponentAnd extends FilterReactor {

	@Override
	public NounMetadata execute() {
		// we want to return a filter object
		// so it can be integrated with the query struct
		AndQueryFilter filter = new AndQueryFilter();
		
		// validate if they are all AND or SIMPLE filters
		boolean consolidate = true;
		int size = this.curRow.size();
		CONSOLIDATE_LOOP : for(int i = 0; i < size; i++) {
			Object v = this.curRow.get(i);
			if(v instanceof IQueryFilter) {
				QUERY_FILTER_TYPE fType = ((IQueryFilter) v).getQueryFilterType();
				if(fType != IQueryFilter.QUERY_FILTER_TYPE.SIMPLE
						&& fType != IQueryFilter.QUERY_FILTER_TYPE.AND) {
					consolidate = false;
					break CONSOLIDATE_LOOP;
				}
			}
		}
		
		if(consolidate) {
			for(int i = 0; i < size; i++) {
				Object v = this.curRow.get(i);
				if(v instanceof SimpleQueryFilter) {
					filter.addFilter((SimpleQueryFilter) v);
				} else if(v instanceof AndQueryFilter) {
					filter.addFilter(((AndQueryFilter) v).getFilterList());
				}
			}
		} else {
			for(int i = 0; i < size; i++) {
				Object v = this.curRow.get(i);
				if(v instanceof IQueryFilter) {
					filter.addFilter((IQueryFilter) v);
				}
			}
		}
		
		return new NounMetadata(filter, PixelDataType.FILTER);
	}

	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		if(parentReactor != null) {
			// filters are added to curRow
			parentReactor.getCurRow().add(execute());
		}
	}
}
