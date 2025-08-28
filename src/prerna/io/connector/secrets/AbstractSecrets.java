/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.io.connector.secrets;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

public abstract class AbstractSecrets implements ISecrets {

	/**
	 * General method to grab input from environment variable or RDF_Map
	 *
	 * @param key
	 * @return
	 */
	protected String getInput(String key) {
		String value = System.getenv(key);
		if (value == null || value.isEmpty()) {
			value = Utility.getDIHelperProperty(key);
		}

		return value;
	}

	/**
	 * @param type
	 * @return
	 */
	protected String getBaseForEngine(IEngine.CATALOG_TYPE type) {
		String inputName = getInputNameForEngine(type);
		return getInput(inputName);
	}

	/**
	 * @param type
	 * @return
	 */
	protected String getInputNameForEngine(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return SECRETS_DB_PATH;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return SECRETS_STORAGE_PATH;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return SECRETS_MODEL_PATH;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return SECRETS_VECTOR_PATH;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return SECRETS_FUNCTION_PATH;
		} else if (IEngine.CATALOG_TYPE.PROJECT == type) {
			return SECRETS_PROJECT_PATH;
		} else if (IEngine.CATALOG_TYPE.VENV == type) {
			return SECRETS_VENV_PATH;
		}

		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
}
