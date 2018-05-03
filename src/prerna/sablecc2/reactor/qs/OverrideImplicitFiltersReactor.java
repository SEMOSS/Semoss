package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;

public class OverrideImplicitFiltersReactor extends AbstractQueryStructReactor {
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		boolean overrideImplicitFilters = true;
		if(!this.curRow.isEmpty()) {
			overrideImplicitFilters = (boolean) this.curRow.get(0);
		}
		this.qs.setOverrideImplicit(overrideImplicitFilters);
		return qs;
	}
	
}
