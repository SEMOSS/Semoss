package prerna.reactor.agent.mcp;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EngineCapabilitiesReactor extends AbstractReactor {
	
	// gets the engine id
	// finds if the engine is there in util
	// finds the type of engine and pumps out a basic json for the capabilities of this engine
	
	public EngineCapabilitiesReactor()
	{
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	
	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		List<Map<String, Object>> output;
//		switch(engineType) {
//			case DATABASE:
//				output = getDatabaseUsage(engineId);
//				break;
//			case STORAGE:
//				output = getStorageUsage(engineId);
//				break;
//			case MODEL:
//				output = getModelUsage(engineId);
//				break;
//			case VECTOR:
//				output = getVectorUsage(engineId);
//				break;
//			case FUNCTION:
//				output = getFunctionUsage(engineId);
//				break;
//			default:
//				output = getPendingUsage();
//				break;
//		}
		return new NounMetadata(new Vector(), PixelDataType.VECTOR);
	}
}
