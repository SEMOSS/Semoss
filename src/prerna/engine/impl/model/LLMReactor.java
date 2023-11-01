package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class LLMReactor extends AbstractReactor {
	
	public LLMReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), 
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String context = this.keyValue.get(this.keysToGet[2]);
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}
		
		Map<String, Object> paramMap = getMap();
		IModelEngine eng = Utility.getModel(engineId);
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		if (paramMap.containsKey("full_prompt")) {
			paramMap.put("full_prompt", Utility.decodeURIComponent((String) paramMap.get("full_prompt")));
		}
		
		
		Map<String, String> output = eng.ask(question, context, this.insight, paramMap);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
}
