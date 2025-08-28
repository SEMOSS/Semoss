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
package prerna.engine.api;

import prerna.engine.impl.venv.PythonVenvEngine;

public enum VenvTypeEnum {
	PYTHON("PYTHON", PythonVenvEngine.class.getName());

	private String venvName;
	private String venvClass;

	VenvTypeEnum(String venvName, String venvClass) {
		this.venvName = venvName;
		this.venvClass = venvClass;
	}

	/**
	 * @return
	 */
	public String getVenvClass() {
		return this.venvClass;
	}

	/**
	 * @return
	 */
	public String getVenvName() {
		return this.venvName;
	}

	/**
	 * @param name
	 * @return
	 */
	public static VenvTypeEnum getEnumFromName(String name) {
		VenvTypeEnum[] allValues = values();
		for (VenvTypeEnum v : allValues) {
			if (v.getVenvName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
