package prerna.engine.impl.modifications;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IDatabase.DATABASE_TYPE;
import prerna.engine.api.IEngineModifier;

public class EngineModificationFactory {

	private EngineModificationFactory() {
		
	}
	
	public static IEngineModifier getEngineModifier(IDatabase engine) {
		DATABASE_TYPE dbType = engine.getDatabaseType();
		
		IEngineModifier modifier = null;
		if(dbType == DATABASE_TYPE.RDBMS) {
			modifier = new RdbmsModifier();
		}
		
		if(modifier != null) {
			modifier.setEngine(engine);
		
		}
		return modifier;
	}
	
}
