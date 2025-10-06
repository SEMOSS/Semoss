package prerna.reactor.model;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetModelAPIReactor extends AbstractReactor{

	public GetModelAPIReactor() {
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
		return new NounMetadata(model.getModelType().getModelName(), PixelDataType.CONST_STRING);
	}
}
