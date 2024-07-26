package prerna.io.connector.secrets;

import prerna.engine.api.IEngine;
import prerna.util.DIHelper;

public abstract class AbstractSecrets implements ISecrets {

	/**
	 * General method to grab input from environment variable or RDF_Map
	 * @param key
	 * @return
	 */
	protected String getInput(String key) {
		String value = System.getenv(key);
		if(value == null || value.isEmpty()) {
			value = DIHelper.getInstance().getProperty(key);
		}

		return value;
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	protected String getBaseForEngine(IEngine.CATALOG_TYPE type) {
		String inputName = getInputNameForEngine(type);
		return getInput(inputName);
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	protected String getInputNameForEngine(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return SECRETS_DB_PATH;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return SECRETS_STORAGE_PATH;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return SECRETS_MODEL_PATH;
		} else if(IEngine.CATALOG_TYPE.VECTOR == type) {
			return SECRETS_VECTOR_PATH;
		} else if(IEngine.CATALOG_TYPE.FUNCTION == type) {
			return SECRETS_FUNCTION_PATH;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return SECRETS_PROJECT_PATH;
		} else if(IEngine.CATALOG_TYPE.VENV == type) {
			return SECRETS_VENV_PATH;
		} 
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
}
