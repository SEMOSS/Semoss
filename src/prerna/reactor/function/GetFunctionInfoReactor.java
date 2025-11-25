package prerna.reactor.function;

import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Returns a string containing info about the function engine. Specifically,
 * the name, description, and parameters.
 */
public class GetFunctionInfoReactor extends AbstractReactor{

	public GetFunctionInfoReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
		}
		
		IFunctionEngine engine = Utility.getFunctionEngine(engineId);
		
		System.out.println(engine.getFunctionName() +
		engine.getFunctionDescription() +
		engine.getParameters().toString() +
		engine.getRequiredParameters().toString());

		
		
		
		return new NounMetadata(null, PixelDataType.CUSTOM_DATA_STRUCTURE);
		
	}
		

}
