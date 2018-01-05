package prerna.sablecc2.reactor.qs.selectors;

import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.ReactorKeysEnum;

public class GroupConcatReactor extends QuerySelectReactor {
	
	public GroupConcatReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
		QueryAggregationEnum aggregationFunction = QueryAggregationEnum.GROUP_CONCAT;
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);
				qs.addSelector(genFunctionSelector(aggregationFunction, innerSelector));
			}
		}
		return qs;
	}
}