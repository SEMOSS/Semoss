package prerna.reactor.model;

import prerna.reactor.AbstractReactor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.NamedEntityRecognitionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PredictReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(PredictReactor.class);
	
	public PredictReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PROMPT.getKey(),
				ReactorKeysEnum.LABELS.getKey(),
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
		
		String prompt = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String labels = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
		List<String> labelsList = parseLabels(labels);
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		
		
		IModelEngine targetModel = Utility.getModel(engineId);
		NamedEntityRecognitionEngine targetEngine = (NamedEntityRecognitionEngine) targetModel;

		Map<String, Object> output = targetEngine.predict(prompt, labelsList, this.insight, paramMap).toMap();
		
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
    private List<String> parseLabels(String labels) {
        if (labels == null || labels.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.stream(labels.split(","))
                     .map(String::trim)
                     .collect(Collectors.toList());
    }

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
