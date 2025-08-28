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
package prerna.reactor.interceptor;

import java.lang.reflect.Method;
import java.util.Map;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class ReactorInputHelper {

	private final NounStore nounStore;
	private final Map<String, Object> argumentsMap; // The map containing ENGINE, METHOD_NAME, CONFIG, etc.

	public ReactorInputHelper(NounStore nounStore) {
		this.nounStore = nounStore;
		// Assuming the main arguments map is stored under
		// PipelineReactorUtils.ARGUMENTS
		GenRowStruct grs = nounStore.getNoun(PipelineReactorUtils.ARGUMENTS);
		if (grs != null && grs.size() > 0) {
			this.argumentsMap = (Map<String, Object>) grs.get(0);
		} else {
			this.argumentsMap = null;
		}
	}

	public IEngine getEngine() {
		if (argumentsMap != null && argumentsMap.containsKey(PipelineReactorUtils.ENGINE)) {
			return (IEngine) argumentsMap.get(PipelineReactorUtils.ENGINE);
		}
		return null;
	}

	public Method getMethod() {
		if (argumentsMap != null && argumentsMap.containsKey(PipelineReactorUtils.METHOD_NAME)) {
			return (Method) argumentsMap.get(PipelineReactorUtils.METHOD_NAME);
		}
		return null;
	}

	public Map<String, Object> getConfig() {
		if (argumentsMap != null && argumentsMap.containsKey(PipelineReactorUtils.CONFIG)) {
			return (Map<String, Object>) argumentsMap.get(PipelineReactorUtils.CONFIG);
		}
		return null;
	}

	/**
	 * Retrieves a configuration parameter from the reactor's config map.
	 *
	 * @param key
	 *            The key of the configuration parameter.
	 * @param type
	 *            The expected class type of the parameter.
	 * @return The configuration parameter, or null if not found or type mismatch.
	 */
	public <T> T getConfigParameter(String key, Class<T> type) {
		Map<String, Object> config = getConfig();
		if (config != null && config.containsKey(key)) {
			Object value = config.get(key);
			if (value != null && type.isInstance(value)) {
				return type.cast(value);
			}
		}
		return null;
	}

	/**
	 * Retrieves a method argument by its name (e.g., "arg0", "question"). This
	 * relies on how arguments are mapped in PipelineInvocationHandler.
	 *
	 * @param argName
	 *            The name of the argument.
	 * @return The argument value, or null if not found.
	 */
	public Object getMethodArgument(String argName) {
		if (argumentsMap != null && argumentsMap.containsKey(argName)) {
			return argumentsMap.get(argName);
		}
		return null;
	}

	/**
	 * Retrieves a method argument by its name and casts it to the specified type.
	 *
	 * @param argName
	 *            The name of the argument.
	 * @param type
	 *            The expected class type of the argument.
	 * @return The argument value, or null if not found or type mismatch.
	 */
	public <T> T getMethodArgument(String argName, Class<T> type) {
		Object value = getMethodArgument(argName);
		if (value != null && type.isInstance(value)) {
			return type.cast(value);
		}
		return null;
	}

	/**
	 * Retrieves the entire argument map
	 *
	 * @return The argument map
	 */
	public Map<String, Object> getArgumentsMap() {
		return argumentsMap;
	}
}
