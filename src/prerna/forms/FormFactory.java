package prerna.forms;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IDatabase.ENGINE_TYPE;

public final class FormFactory {

	private FormFactory() {
		
	}
	
	public static AbstractFormBuilder getFormBuilder(IDatabase engine) {
		ENGINE_TYPE eType = engine.getEngineType();
		if(eType == ENGINE_TYPE.JENA || eType == ENGINE_TYPE.SESAME) {
			return new RdfFormBuilder(engine);
		} else {
			return null;
		}
	}
}
