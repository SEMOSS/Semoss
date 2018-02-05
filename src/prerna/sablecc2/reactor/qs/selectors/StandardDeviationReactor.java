package prerna.sablecc2.reactor.qs.selectors;

import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StandardDeviationReactor extends QuerySelectReactor {
	
	public StandardDeviationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);
				qs.addSelector(genFunctionSelector(QueryFunctionHelper.STDEV_1, innerSelector));
			}
		}
		return qs;
	}
}