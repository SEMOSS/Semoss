package prerna.reactor.model;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetContextWindowReactor extends AbstractReactor{
	
	public GetContextWindowReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.MODEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), modelId)) {
			throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to it");
		}
		
		IModelEngine model = Utility.getModel(modelId);
		
		int contextWindow = model.getContextWindow();
		
		return new NounMetadata(contextWindow, PixelDataType.CONST_INT);
	}
	
	@Override
	public String getReactorDescription() {
		return "This method is used to return the context window for a given model. If the model does not have a context window value set it will return 0.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MODEL.getKey())) {
			return "This is the ID for the model";
		} 
		return super.getDescriptionForKey(key);
	}
}
