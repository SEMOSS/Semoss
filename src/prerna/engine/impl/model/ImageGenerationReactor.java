package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IImageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/*
 * ImageGeneration("6154866e-4h22-898j-q33b-47v6w9g55re4", "Dogs playing poker.", space="6d143c32-9a2e-415c-88ba-a739475bab3b", filePath="/images") 
*/

public class ImageGenerationReactor extends AbstractReactor {
	
	public ImageGenerationReactor( ) {
		this.keysToGet = new String[] {
									ReactorKeysEnum.ENGINE.getKey(), 
									ReactorKeysEnum.PROMPT.getKey(),
									ReactorKeysEnum.SPACE.getKey(),
									ReactorKeysEnum.FILE_PATH.getKey(),
									ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
									};
		this.keyRequired = new int[] {1, 1, 0, 0, 0};
		}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		String prompt = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		
		Map<String, Object> paramMap = getMap();
		IImageEngine eng = Utility.getImageEngine(engineId);
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		// Check if space or filePath are included in param dict, if not pass as empty strings
		String space = this.keyValue.get(keysToGet[2]) != null ? Utility.decodeURIComponent(this.keyValue.get(keysToGet[2])) : "";
		String filePath = this.keyValue.get(keysToGet[3]) != null ? Utility.decodeURIComponent(this.keyValue.get(keysToGet[3])) : "";
		
		paramMap.put("space", space);
		paramMap.put("filePath", filePath);
		
		Map<String, Object> output = eng.generateImage(prompt, this.insight, paramMap).toMap();
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
