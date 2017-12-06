package prerna.sablecc2.reactor.expression.filter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class OpOr extends AbstractOpFiltering {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		if(isQuery()) {
			// we want to return a filter object
			// so it can be integrated with the query struct
			OrQueryFilter filter = new OrQueryFilter();
			for(Object v : values) {
				if(v instanceof IQueryFilter) {
					filter.addFilter((IQueryFilter)v);
				}
			}
			return new NounMetadata(filter, PixelDataType.FILTER);
		}
		
		boolean result = eval(values);
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}
	
	public boolean eval(Object... values) {
		boolean result = false;
		for (Object booleanValue : values) {
			// need only 1 value to be true
			// in order to return true
			if((boolean) booleanValue) {
				result = true;
				break;
			}
		}
		return result;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "boolean";
	}
}
