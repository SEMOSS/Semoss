package prerna.reactor.qs.selectors;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UniqueCountReactor extends SelectReactor {
	
	public UniqueCountReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);
				qs.addSelector(genFunctionSelector(QueryFunctionHelper.UNIQUE_COUNT, innerSelector, true));
			}
		}
		return qs;
	}
}