package prerna.sablecc2.reactor.qs.filter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryFilterComponentOr extends FilterReactor {

	@Override
	public NounMetadata execute() {
		// we want to return a filter object
		// so it can be integrated with the query struct
		OrQueryFilter filter = new OrQueryFilter();
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			Object v = this.curRow.get(i);
			if(v instanceof IQueryFilter) {
				filter.addFilter((IQueryFilter) v);
			}
		}
		return new NounMetadata(filter, PixelDataType.FILTER);
	}


}
