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
package prerna.engine.impl.function;

public class FunctionParameter {

	String parameterName;
	String parameterType;
	String parameterDescription;

	/** */
	public FunctionParameter() {
	}

	/**
	 * @param parameterName
	 * @param parameterType
	 * @param parameterDescription
	 */
	public FunctionParameter(String parameterName, String parameterType, String parameterDescription) {
		this.parameterName = parameterName;
		this.parameterType = parameterType;
		this.parameterDescription = parameterDescription;
	}

	/**
	 * @return
	 */
	public String getParameterName() {
		return parameterName;
	}

	/**
	 * @param parameterName
	 */
	public void setParameterName(String parameterName) {
		this.parameterName = parameterName;
	}

	/**
	 * @return
	 */
	public String getParameterType() {
		return parameterType;
	}

	/**
	 * @param parameterType
	 */
	public void setParameterType(String parameterType) {
		this.parameterType = parameterType;
	}

	/**
	 * @return
	 */
	public String getParameterDescription() {
		return parameterDescription;
	}

	/**
	 * @param parameterDescription
	 */
	public void setParameterDescription(String parameterDescription) {
		this.parameterDescription = parameterDescription;
	}
}
