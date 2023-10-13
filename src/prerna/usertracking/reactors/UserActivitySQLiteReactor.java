package prerna.usertracking.reactors;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;

public class UserActivitySQLiteReactor extends AbstractQueryStructReactor {

	// UserActivity example
	// UserActivity ( ) | Import ( frame = [ CreateFrame ( frameType = [ GRID ] , override = [ true ] ) .as ( [ "FRAME961184" ] ) ] ) ;
	// Frame ( frame = [ FRAME961184 ] ) | UserActivity ( ) | AutoTaskOptions ( panel = [ "0" ] , layout = [ "Grid" ] ) | Collect ( 2000 ) ;
	
	// date format function example
	// Frame ( frame = [ FRAME961184 ] ) | Select ( DateFormat ( "%Y-%m-%d" , DATE_CREATED ) ) | CollectAll ( ) ;
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		this.qs.setEngineId("UserTrackingDatabase");
		this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

		SelectQueryStruct sQs = new SelectQueryStruct();
		// selectors
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("COUNT");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "USERID"));
		sQs.addSelector(fSelector);

		sQs.addSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
//		// group by
		sQs.addGroupBy(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
		// order by
		sQs.addOrderBy("USER_TRACKING" + "__" + "CREATED_ON", "DESC");;
		this.qs.merge(sQs);
		return this.qs;
	}
}
