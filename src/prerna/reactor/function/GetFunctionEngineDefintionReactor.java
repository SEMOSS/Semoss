package prerna.reactor.function;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetFunctionEngineDefintionReactor extends AbstractReactor {

	public GetFunctionEngineDefintionReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(getUnableToAccessError(engineId));
		}
		
		IFunctionEngine engine = Utility.getFunctionEngine(engineId);
		return new NounMetadata(engine.getFunctionDefintionJson(), PixelDataType.JSON_OBJECT);
	}
	
	String getUnableToAccessError(String engineId) {
		return "Function Engine " + engineId + " does not exist or user does not have access to this function";
	}
	
}
