package prerna.query.querystruct.filters;

import java.util.List;

public class AndQueryFilter extends AbstractListFilter {

	/*
	 * This classes used the filter list in the parent	
	 */
	
	public AndQueryFilter() {
		super();
	}
	
	public AndQueryFilter(List<IQueryFilter> filterList) {
		super(filterList);
	}
	
	public AndQueryFilter(IQueryFilter... filterList ) {
		super(filterList);
	}

	@Override
	public IQueryFilter copy() {
		List<IQueryFilter> cList = copy(this.filterList);
		return new AndQueryFilter(cList);
	}

	@Override
	public QUERY_FILTER_TYPE getQueryFilterType() {
		return QUERY_FILTER_TYPE.AND;
	}
	
}
