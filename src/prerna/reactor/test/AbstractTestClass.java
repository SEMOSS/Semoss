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
package prerna.reactor.test;

import java.util.HashMap;
import java.util.Map;

/**
 * This is just a internal class so that when we compile to execute the
 * assimilator we can have a method to call based on the super that is assigned
 * to the new class
 */
public abstract class AbstractTestClass {

	Map<String, Object> variables = new HashMap<>();

	/**
	 * Method that return the evaluation of the signature
	 *
	 * @return
	 */
	public void execute() {
	}

	public Map<String, Object> getVariables() {
		return this.variables;
	}

	public void addVariable(String var) {
		variables.put(var, 1);
	}

	public void addVariable(String var, int value) {
		variables.put(var, value);
	}

	public Object getVariable(String var) {
		return variables.get(var);
	}
}
