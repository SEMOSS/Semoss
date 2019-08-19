package prerna.engine.impl.modifications;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IEngineModifier;

public class EngineModificationFactory {

	private EngineModificationFactory() {
		
	}
	
	public static IEngineModifier getEngineModifier(IEngine engine) {
		ENGINE_TYPE eType = engine.getEngineType();
		
		IEngineModifier modifier = null;
		if(eType == ENGINE_TYPE.RDBMS) {
			modifier = new RdbmsModifier();
		}
		
		if(modifier != null) {
			modifier.setEngine(engine);
		
		}
		return modifier;
	}
	
}
