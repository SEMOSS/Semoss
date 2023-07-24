package prerna.engine.impl.modifications;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IDatabase.ENGINE_TYPE;
import prerna.engine.api.IEngineModifier;

public class EngineModificationFactory {

	private EngineModificationFactory() {
		
	}
	
	public static IEngineModifier getEngineModifier(IDatabase engine) {
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
