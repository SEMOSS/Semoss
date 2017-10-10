package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.IQuerySelector;
import prerna.query.querystruct.QueryAggregationEnum;
import prerna.query.querystruct.QueryMathSelector;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;

public class CountReactor extends SelectReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		QueryAggregationEnum aggregationFunction = QueryAggregationEnum.COUNT;
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);

				QueryMathSelector newSelector = new QueryMathSelector();
				newSelector.setInnerSelector(innerSelector);
				newSelector.setMath(aggregationFunction);
				qs.addSelector(newSelector);
				
			}
		}
		return qs;
	}
}