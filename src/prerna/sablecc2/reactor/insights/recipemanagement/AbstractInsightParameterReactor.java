package prerna.sablecc2.reactor.insights.recipemanagement;

import java.util.List;
import java.util.Map;

import prerna.query.parsers.ParamStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightParameterReactor extends AbstractReactor {

	/**
	 * Get the input map
	 * @return
	 */
	protected Map<String, Object> getParamMap() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PARAM_STRUCT.getKey());
		if(grs != null && !grs.isEmpty()) {
			Map<String, Object> mapInput = (Map<String, Object>) grs.get(0);
			return mapInput;
		}
		
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		
		throw new NullPointerException("Could not find the input for the param struct map");
	}
	
	/**
	 * Generate the param struct object
	 * @param inputMap
	 * @return
	 */
	protected ParamStruct generateParamStruct(Map<String, Object> inputMap) {
		
		return null;
	}
}
