/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
