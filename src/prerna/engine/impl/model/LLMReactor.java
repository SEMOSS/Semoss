package prerna.engine.impl.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
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
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(),
				 ReactorKeysEnum.USE_HISTORY.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		// default is true
		Boolean useHistoryParam = Boolean.parseBoolean(this.keyValue.getOrDefault(ReactorKeysEnum.USE_HISTORY.getKey(), "true")+"");

		String question = Utility.decodeURIComponent(ReactorKeysEnum.COMMAND.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}
		
		Map<String, Object> paramMap = getMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		paramMap.put("use_history", useHistoryParam);
		

		Map<String, Object> output = modelEngine.ask(question, context, this.insight, paramMap).toMap();
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
	
	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if(key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if(key.equals(ReactorKeysEnum.USE_HISTORY.getKey())) {
			return "Boolean true/false to determine if we should incorporate the user's chat history based on the previous chats in this insight id";
		} else if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc. "
					+ "In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for " + 
					Arrays.asList(ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey());
		}
		
		return super.getDescriptionForKey(key);
	}
	
}
