package prerna.reactor.insights.recipemanagement;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteInsightParameterReactor extends AbstractInsightParameterReactor {

	public DeleteInsightParameterReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PARAM_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String paramName = this.keyValue.get(this.keysToGet[0]);
		NounMetadata removed = this.insight.getVarStore().remove(VarStore.PARAM_STRUCT_PREFIX + paramName);
		return removed;
	}

}
