package prerna.sablecc2.reactor.insights.recipemanagement;

import java.util.Map;

import prerna.query.parsers.ParamStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateInsightParameterReactor extends AbstractInsightParameterReactor {

	public UpdateInsightParameterReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PARAM_STRUCT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the parameter as a map of key=value pairs
		Map<String, Object> paramMap = getParamMap();
		// turn this into a param struct object
		ParamStruct paramStruct = ParamStruct.generateParamStruct(paramMap);
		String paramName = paramStruct.getParamName();
		// parameter name must be defined
		if(paramName == null || paramName.isEmpty()) {
			throw new IllegalArgumentException("Parameter name is not defined");
		}
		// parameter shouldn't already exists
		String variableName = VarStore.PARAM_STRUCT_PREFIX + paramName;
		if(!this.insight.getVarStore().getInsightParameterKeys().contains(variableName)) {
			throw new IllegalArgumentException("Could not find parameter with name = " + paramName);
		}
		
		NounMetadata pStructNoun = new NounMetadata(paramStruct, PixelDataType.PARAM_STRUCT);
		this.insight.getVarStore().put(variableName, pStructNoun);
		return pStructNoun;
	}

}
