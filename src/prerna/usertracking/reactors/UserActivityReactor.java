package prerna.usertracking.reactors;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;

public class UserActivityReactor extends AbstractQueryStructReactor {

	// UserActivity example
	// UserActivityH2 ( ) | Import ( frame = [ CreateFrame ( frameType = [ GRID ] , override = [ true ] ) .as ( [ "FRAME961184" ] ) ] ) ;
	// Frame ( frame = [ FRAME961184 ] ) | UserActivityH2 ( ) | AutoTaskOptions ( panel = [ "0" ] , layout = [ "Grid" ] ) | Collect ( 2000 ) ;
	
	// date format function example
	// Frame ( frame = [ FRAME961184 ] ) | Select ( DateFormat ( DATE_CREATED, "YYYY-MM-dd" ) ) | CollectAll ( ) ;
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		this.qs.setEngineId(Constants.USER_TRACKING_DB);
		this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

		SelectQueryStruct sQs = new SelectQueryStruct();
		// selectors
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("COUNT");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "USERID"));
		sQs.addSelector(fSelector);

		fSelector = new QueryFunctionSelector();
		fSelector.setAlias("CREATED_ON");
		fSelector.setFunction(QueryFunctionHelper.DATE_FORMAT);
		fSelector.addInnerSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
		fSelector.addInnerSelector(new QueryConstantSelector("yyyy-MM-dd"));
		sQs.addSelector(fSelector);
		// group by
		fSelector = new QueryFunctionSelector();
		fSelector.setAlias("CREATED_ON");
		fSelector.setFunction(QueryFunctionHelper.DATE_FORMAT);
		fSelector.addInnerSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
		fSelector.addInnerSelector(new QueryConstantSelector("yyyy-MM-dd"));
		sQs.addGroupBy(fSelector);
		this.qs.merge(sQs);
		return this.qs;
	}
}
