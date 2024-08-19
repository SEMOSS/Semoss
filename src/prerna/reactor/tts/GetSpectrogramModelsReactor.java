package prerna.reactor.tts;

import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.ISpeechEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetSpectrogramModelsReactor extends AbstractReactor {
	
	public GetSpectrogramModelsReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey()
		};
		
		this.keyRequired = new int[] { 1 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		ISpeechEngine eng = Utility.getSpeechEngine(engineId);
		Map<String, Object> output = eng.getSpectrogramModels(this.insight);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
		
	}


}
