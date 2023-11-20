package prerna.reactor.vector;

import java.util.Properties;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GetModelMaxTokenLength extends AbstractReactor {
	
	public GetModelMaxTokenLength() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] {1};
	}
	

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
		}
				
		IModelEngine modelEngine = Utility.getModel(engineId);
		Properties modelProperties = modelEngine.getSmssProp();

		String maxTokens = "512";
		if (!modelProperties.isEmpty() && modelProperties.containsKey(Constants.MAX_TOKENS)) {
			maxTokens = modelProperties.getProperty(Constants.MAX_TOKENS);
		}
		
		return new NounMetadata(maxTokens, PixelDataType.CONST_STRING);
	}
}
