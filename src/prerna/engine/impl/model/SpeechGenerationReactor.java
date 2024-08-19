package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.ISpeechEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SpeechGenerationReactor extends AbstractReactor {
	
	public SpeechGenerationReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.PROMPT.getKey(),
				// The space of the speaker file to be cloned
				ReactorKeysEnum.ASSET_SPACE.getKey(),
				// The path to the speaker file to be cloned
				ReactorKeysEnum.ASSET_FILE_PATH.getKey(),
				// Output dir (optional)
				ReactorKeysEnum.SPACE.getKey(),
				// Output dir path (optional)
				ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] {1, 1, 1, 1, 0, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}

		String prompt = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		ISpeechEngine eng = Utility.getSpeechEngine(engineId);
		
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		String speakerSpace = this.keyValue.get(keysToGet[2]);
		String speakerFilePath = this.keyValue.get(keysToGet[3]);

		// Check if space or filePath are included in param dict, if not pass as empty strings
		String space = this.keyValue.get(keysToGet[4]) != null ? Utility.decodeURIComponent(this.keyValue.get(keysToGet[4])) : "";
		String filePath = this.keyValue.get(keysToGet[5]) != null ? Utility.decodeURIComponent(this.keyValue.get(keysToGet[5])) : "";
		
		// Having the engine resolve these paths since the engine can be called directly from python which cannot yet resolve these paths
		paramMap.put("space", space);
		paramMap.put("filePath", filePath);
		paramMap.put("speakerSpace", speakerSpace);
		paramMap.put("speakerFilePath", speakerFilePath);
		
		Map<String, Object> output = eng.generateSpeech(prompt, this.insight, paramMap).toMap();
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
