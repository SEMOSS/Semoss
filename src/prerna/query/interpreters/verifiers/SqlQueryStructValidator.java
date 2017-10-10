//package prerna.query.interpreters.verifiers;
//
//import java.util.List;
//
//import prerna.query.querystruct.IQuerySelector;
//import prerna.query.querystruct.QueryAggregationEnum;
//import prerna.query.querystruct.QueryColumnSelector;
//import prerna.query.querystruct.QueryStruct2;
//
//public class SqlQueryStructValidator implements IQueryStructValidator {
//
//	// the original QS
//	private QueryStruct2 originalQs;
//	private Boolean isValid = null;
//	
//	public SqlQueryStructValidator(QueryStruct2 qs) {
//		this.originalQs = qs;
//	}
//	
//	@Override
//	public boolean isValid() {
//		if(this.isValid == null) {
//			validate();
//		}
//		return this.isValid;
//	}
//
//	private void validate() {
//		// right now, the main thing to validate is math being performed on the headers
//		List<IQuerySelector> querySelectors = this.originalQs.getSelectors();
//		for(IQuerySelector selector : querySelectors) {
//			QueryAggregationEnum math = selector.getMath();
//			// if no math, no problem
//			if(math == null) {
//				continue;
//			}
//			// else, validate the math we allow
//			if(!QueryAggregationEnum.isValid(math.getBaseSqlSyntax())) {
//				this.isValid = false;
//			}
//		}
//		
//		// we haven't run into anything bad, we are good to go
//		this.isValid = true;
//	}
//
//	@Override
//	public QueryStruct2 getQueryableQueryStruct() {
//		if(this.isValid == null) {
//			validate();
//		}
//		if(this.isValid) {
//			return this.originalQs;
//		}
//		
//		// it is not completely valid
//		// separate out into 2 parts
//		QueryStruct2 sqlQs = new QueryStruct2();
//
//		// loop through and add all the headers that are valid
//		List<IQuerySelector> querySelectors = this.originalQs.getSelectors();
//		for(IQuerySelector selector : querySelectors) {
//			QueryAggregationEnum math = selector.getMath();
//			// if there is invalid math
//			// we want to add it in
//			if(math == null) {
//				sqlQs.addSelector(selector);
//			}
//			
//			// else, validate the math we allow
//			if(QueryAggregationEnum.isValid(math.getBaseSqlSyntax())) {
//				sqlQs.addSelector(selector);
//			}
//		}
//		
//		// merge everything else
//		sqlQs.mergeFilters(this.originalQs.getFilters());
//		sqlQs.mergeRelations(this.originalQs.getRelations());
//		sqlQs.mergeGroupBy(this.originalQs.getGroupBy());
//		sqlQs.mergeOrderBy(this.originalQs.getOrderBy());
//		return sqlQs;
//	}
//
//	@Override
//	public QueryStruct2 getNonQueryableQueryStruct() {
//		QueryStruct2 nonSqlQs = new QueryStruct2();
//		if(this.isValid == null) {
//			validate();
//		}
//		if(this.isValid) {
//			// everything is valid
//			// return an empty QS
//			return nonSqlQs;
//		}
//
//		// it is not completely valid
//		// separate out into 2 parts
//
//		// loop through and add all the headers that are valid
//		List<IQuerySelector> querySelectors = this.originalQs.getSelectors();
//		for(IQuerySelector selector : querySelectors) {
//			QueryAggregationEnum math = selector.getMath();
//			// if there is invalid math
//			// we want to add it in
//			if(!QueryAggregationEnum.isValid(math.getBaseSqlSyntax())) {
//				nonSqlQs.addSelector(selector);
//			}
//		}
//		
//		// merge everything else
//		nonSqlQs.mergeFilters(this.originalQs.getFilters());
//		nonSqlQs.mergeRelations(this.originalQs.getRelations());
//		List<QueryColumnSelector> groupBys = this.originalQs.getGroupBy();
//		nonSqlQs.mergeGroupBy(groupBys);
//		
//		// we need to get the group by selectors
//		// and add them as normal selectors so we can properly join
//		// between the valid and not valid values
//		for(QueryColumnSelector gSelector : groupBys) {
//			nonSqlQs.addSelector(gSelector);
//		}
//		
//		nonSqlQs.mergeOrderBy(this.originalQs.getOrderBy());
//		return nonSqlQs;
//	}
//	
//
//}
