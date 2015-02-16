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
package prerna.rdf.engine.wrappers;

import java.util.Hashtable;

import prerna.rdf.engine.api.ISelectStatement;

public class SelectStatement implements ISelectStatement {
	
	transient public Hashtable propHash = new Hashtable();
	transient public Hashtable rawPropHash = new Hashtable();
	String serialRep = null;

	public Object getVar(Object var) {
		Object retVal = propHash.get(var);
		return retVal;
	}

	public Object getRawVar(Object var) {
		// TODO Auto-generated method stub
		return rawPropHash.get(var);
	}

	public void setPropHash(Hashtable propHash) {
		this.propHash = propHash;
	}

	public void setRPropHash(Hashtable rawPropHash) {
		// TODO Auto-generated method stub
		this.rawPropHash = rawPropHash;
	}

	public Hashtable getPropHash() {
		// TODO Auto-generated method stub
		return propHash;
	}

	public Hashtable getRPropHash() {
		// TODO Auto-generated method stub
		return rawPropHash;
	}

	
	@Override
	public void setVar(Object key, Object value) {
		propHash.put(key, value);
		
	}

	@Override
	public void setRawVar(Object key, Object value) {
		rawPropHash.put(key, value);		
	}

}
