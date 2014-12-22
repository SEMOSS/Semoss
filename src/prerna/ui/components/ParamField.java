/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components;

import javax.swing.JTextField;

/**
 * This provides a text field for the parameter name.
 */
public class ParamField extends JTextField {
	
	String paramName = null;
	String paramType = null;
	
	/**
	 * Constructor for ParamField.
	 * @param paramName String
	 */
	public ParamField(String paramName)
	{
		setParamName(paramName);
		setText("TBD");
	}
	
	/**
	 * Sets the name of the parameter.
	 * @param paramName 	Parameter name.
	 */
	public void setParamName(String paramName)
	{
		this.paramName = paramName;
	}
	
	/**
	 * Gets the name of the parameter.
	
	 * @return String	Parameter name. */
	public String getParamName()
	{
		return this.paramName;
	}

}
