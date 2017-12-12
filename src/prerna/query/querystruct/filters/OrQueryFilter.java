package prerna.query.querystruct.filters;

import java.util.List;

public class OrQueryFilter extends AbstractListFilter {

	/*
	 * This classes used the filter list in the parent	
	 */
	
	public OrQueryFilter() {
		super();
	}
	
	public OrQueryFilter(List<IQueryFilter> filterList) {
		super(filterList);
	}
	
	public OrQueryFilter(IQueryFilter... filterList ) {
		super(filterList);
	}

	@Override
	public IQueryFilter copy() {
		List<IQueryFilter> cList = copy(this.filterList);
		return new OrQueryFilter(cList);
	}
	
	@Override
	public QUERY_FILTER_TYPE getQueryFilterType() {
		return QUERY_FILTER_TYPE.OR;
	}

}
