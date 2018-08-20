package prerna.sablecc2.reactor.qs.selectors;

import java.util.Set;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenericSelectorFunctionReactor extends QuerySelectReactor {

	private String function = null;
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		QueryFunctionSelector functionSelector = null;
		
		// try to create the function selector
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);
				functionSelector = genFunctionSelector(function, innerSelector);
				qs.addSelector(functionSelector);
			}
		}
		
		if(functionSelector != null) {
			Set<String> keys = this.store.getNounKeys();
			for(String key : keys) {
				if(key.equals("all")) {
					continue;
				}
				GenRowStruct grs = this.store.getNoun(key);
				int num = grs.size();
				Object[] additionalParams = new Object[num+1];
				additionalParams[0] = key;
				for(int i = 0; i < num; i++) {
					additionalParams[i+1] = grs.get(i);
				}
				
				functionSelector.addAdditionalParam(additionalParams);
			}
		}
		return qs;
	}
	
	public void setFunction(String function) {
		this.function = function;
	}
}
