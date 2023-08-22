package prerna.forms;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;

public final class FormFactory {

	private FormFactory() {
		
	}
	
	public static AbstractFormBuilder getFormBuilder(IDatabaseEngine engine) {
		DATABASE_TYPE dbType = engine.getDatabaseType();
		if(dbType == DATABASE_TYPE.JENA || dbType == DATABASE_TYPE.SESAME) {
			return new RdfFormBuilder(engine);
		} else {
			return null;
		}
	}
}
