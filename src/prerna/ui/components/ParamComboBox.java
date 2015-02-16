/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components;


import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;

/**
 * This class allows the user to pick parameters by combining a button and a drop-down list.
 */
public class ParamComboBox extends JComboBox {
	
	String fieldName = null;
	Hashtable paramHash = new Hashtable();
	Vector<String> dependency = null;
	String query;
	String type;
	
	/**
	 * Constructor for ParamComboBox.
	 * @param array String[]
	 */
	public ParamComboBox(String[] array)
	{
		super(array);
	}
	
	/**
	 * Sets the name of the parameter.
	 * @param fieldName 	Parameter name.
	 */
	public void setParamName(String fieldName)
	{
		this.fieldName = fieldName;
	}

	/**
	 * Gets the name of the parameter.
	
	 * @return String 	Parameter name. */
	public String getParamName()
	{
		return this.fieldName;
	}
	/**
	 * Sets the data and model for the default combo box.
	 * @param paramHash Hashtable of parameters.
	 * @param nameVector List of names.
	 */
	public void setData(Hashtable paramHash, Vector<String> nameVector)
	{
		this.paramHash = paramHash;
		DefaultComboBoxModel model = new DefaultComboBoxModel(nameVector);
		this.setModel(model);
	}
	
	/**
	 * Gets the URIs based on keys of the parameters hashtable.
	 * @param key 			Parameter whose URI the user wants.
	
	 * @return String		URI. */
	public String getURI(String key)
	{
		String uri = (String) paramHash.get(key);
		return uri;
	}
	
	/**
	 * Sets dependencies.
	 * @param dep	List of dependencies.
	 */
	public void setDependency(Vector<String> dep)
	{
		this.dependency = dep;
	}
	
	/**
	 * Sets the query for execution.
	 * @param query 	Query to be run.
	 */
	public void setQuery(String query)
	{
		this.query = query;
	}
}
