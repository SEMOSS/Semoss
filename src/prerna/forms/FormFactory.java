package prerna.forms;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;

public final class FormFactory {

	private FormFactory() {
		
	}
	
	public static AbstractFormBuilder getFormBuilder(IEngine engine) {
		ENGINE_TYPE eType = engine.getEngineType();
		if(eType == ENGINE_TYPE.JENA || eType == ENGINE_TYPE.SESAME) {
			return new RdfFormBuilder(engine);
		} else {
			return null;
		}
	}
}
