package prerna.reactor.qs.filter;
//package prerna.sablecc2.reactor.qs.filter;
//
//import prerna.query.querystruct.filters.FunctionQueryFilter;
//import prerna.query.querystruct.selectors.QueryFunctionSelector;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//
//public class QueryFilterFunctionComponent extends FilterReactor {
//
//	@Override
//	public NounMetadata execute() {
//		// we want to return a filter object
//		// so it can be integrated with the query struct
//		FunctionQueryFilter filter = new FunctionQueryFilter();
//		int size = this.curRow.size();
//		for(int i = 0; i < size; i++) {
//			Object v = this.curRow.get(i);
//			if(v instanceof QueryFunctionSelector) {
//				filter.setFunctionSelector((QueryFunctionSelector) v);
//			}
//		}
//		return new NounMetadata(filter, PixelDataType.FILTER);
//	}
//	
//}
