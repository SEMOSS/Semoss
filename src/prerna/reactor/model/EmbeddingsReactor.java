package prerna.reactor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EmbeddingsReactor extends AbstractReactor {
	
	public EmbeddingsReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.ENGINE.getKey(), 
			ReactorKeysEnum.VALUES.getKey(), 
			ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
		};
		this.keyRequired = new int[] {1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		List<String> stringsToEmbed = getInputStrings();
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		IModelEngine engine = Utility.getModel(engineId);
		Object output = engine.embeddings(stringsToEmbed, this.insight, paramMap);
		return new NounMetadata(output, PixelDataType.VECTOR);
	}
	
	/**
	 * Get input strings to embed
	 * @return list of engines to delete
	 */
	public List<String> getInputStrings() {
		List<String> inputStrings = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				inputStrings.add(grs.get(i).toString());
			}
			return inputStrings;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			inputStrings.add(this.curRow.get(i).toString());
		}
		
		return inputStrings;
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[2]);
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
		return "This reactor is used to interact with Embedding Model Engines. If the model does not support embeddings " +
				"it will return \"This model does not support embeddings.\"";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.VALUES.getKey())) {
			return "Specify the string value(s) serving as input text, from which you aim to generate embeddings vector(s).";
		} 
		return super.getDescriptionForKey(key);
	}
}
