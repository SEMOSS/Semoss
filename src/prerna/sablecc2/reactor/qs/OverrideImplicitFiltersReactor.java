package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.QueryStruct2;

public class OverrideImplicitFiltersReactor extends AbstractQueryStructReactor {
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		boolean overrideImplicitFilters = true;
		if(!this.curRow.isEmpty()) {
			overrideImplicitFilters = (boolean) this.curRow.get(0);
		}
		this.qs.setOverrideImplicit(overrideImplicitFilters);
		return qs;
	}
	
}
