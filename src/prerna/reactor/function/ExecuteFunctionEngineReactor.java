package prerna.reactor.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExecuteFunctionEngineReactor extends AbstractReactor {

	public ExecuteFunctionEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Fucntion Engine " + engineId + " does not exist or user does not have access to this function");
		}
		
		Map<String, Object> parameterValues = getMap();
		
		IFunctionEngine engine = Utility.getFunctionEngine(engineId);
		Object execValue = engine.execute(parameterValues);
		return new NounMetadata(execValue, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		Map<String, Object> parameterValues = new HashMap<>();

		GenRowStruct mapGrs = this.store.getNoun(this.keysToGet[1]);
		if(mapGrs != null && !mapGrs.isEmpty()) {
			for(int i = 0; i < mapGrs.size(); i++) {
				NounMetadata noun = mapGrs.getNoun(i);
				parameterValues.putAll( (Map<String, Object>) noun.getValue() );
			}
		} else {
			List<Object> mapValues = curRow.getValuesOfType(PixelDataType.MAP);
			if(mapValues != null && !mapValues.isEmpty()) {
				for(int i = 0; i < mapValues.size(); i++) {
					parameterValues.putAll( (Map<String, Object>) mapValues.get(i) );
				}
			}
		}
		
		return parameterValues;
	}

}
