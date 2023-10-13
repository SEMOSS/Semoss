package prerna.reactor.qs.selectors;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenericSelectorFunctionReactor extends SelectReactor {

	private String function = null;
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		// try to create the function selector
		List<IQuerySelector> innerSelectors = new Vector<IQuerySelector>();
		GenRowStruct qsInputs = this.getCurRow();
		if(qsInputs != null && !qsInputs.isEmpty()) {
			for(int selectIndex = 0;selectIndex < qsInputs.size();selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector innerSelector = getSelector(input);
				innerSelectors.add(innerSelector);
			}
		}
		
		QueryFunctionSelector functionSelector = genFunctionSelector(function, innerSelectors);
		qs.addSelector(functionSelector);
		Set<String> keys = this.store.getNounKeys();
		for(String key : keys) {
			if(key.equals("all")) {
				continue;
			} else if(key.equals("sDataType")) {
				String dataType = this.store.getNoun(key).get(0).toString();
				functionSelector.setDataType(dataType);
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
		
		return qs;
	}
	
	public void setFunction(String function) {
		this.function = function;
	}
}
