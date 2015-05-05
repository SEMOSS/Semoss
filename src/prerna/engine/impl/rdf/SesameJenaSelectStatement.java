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
package prerna.engine.impl.rdf;

import java.util.Hashtable;

/**
 * A way of storing each triple in RDF for SELECT SPARQL queries.
 */
@Deprecated
public class SesameJenaSelectStatement {
	
	transient public Hashtable propHash = new Hashtable();
	transient public Hashtable rawPropHash = new Hashtable();
	String serialRep = null;
	public boolean remote = false;
	
	
	/**
	 * Method setVar.  Sets the variables and the values.
	 * @param var Object - The variable name.
	 * @param val Object - The value of the variable.
	 */
	public void setVar(Object var, Object val)
	{
		propHash.put(var, val);
			
	}
	
	/**
	 * Method getVar.  Gets the value associated with a certain variable name.
	 * @param var Object - The variable name.
	
	 * @return Object - the value of the variable. */
	public Object getVar(Object var)
	{
		Object retVal = propHash.get(var);
		return retVal;
	}
	
	/**
	 * Method setRawVar.  Sets the variables and the values. Uses the full URI.
	 * @param var Object - The variable name.
	 * @param val Object - The value of the variable.
	 */
	public void setRawVar(Object var, Object val)
	{
		rawPropHash.put(var, val);
	}
	
	/**
	 * Method getVar.  Gets the value associated with a certain variable name. Uses the full URI.
	 * @param var Object - The variable name.
	
	 * @return Object - the value of the variable. */
	public Object getRawVar(Object var)
	{
		Object retVal = rawPropHash.get(var);
		return retVal;
	}
	
	public Hashtable getPropHash()
	{
		return propHash;
	}
	
	public void setPropHash(Hashtable propHash)
	{
		this.propHash = propHash;
	}
	
	public void setRawPropHash(Hashtable rawPropHash)
	{
		this.rawPropHash = rawPropHash;
	}

	public String getSerialRep() {
		return serialRep;
	}

	public void setSerialRep(String propHashRep) {
		this.serialRep = propHashRep;
	}
}
