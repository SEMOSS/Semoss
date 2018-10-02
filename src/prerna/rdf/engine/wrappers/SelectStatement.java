/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectStatement;

public class SelectStatement  implements ISelectStatement {
	
	transient public Map propHash = new LinkedHashMap();
	transient public Map rawPropHash = new LinkedHashMap();
	String serialRep = null;

	public Object getVar(Object var) {
		Object retVal = propHash.get(var);
		return retVal;
	}

	public Object getRawVar(Object var) {
		// TODO Auto-generated method stub
		return rawPropHash.get(var);
	}

	public void setPropHash(Map propHash) {
		this.propHash = propHash;
	}

	public void setRPropHash(Map rawPropHash) {
		// TODO Auto-generated method stub
		this.rawPropHash = rawPropHash;
	}

	public Map getPropHash() {
		// TODO Auto-generated method stub
		return propHash;
	}

	public Map getRPropHash() {
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((propHash == null) ? 0 : propHash.hashCode());
		result = prime * result
				+ ((rawPropHash == null) ? 0 : rawPropHash.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SelectStatement other = (SelectStatement) obj;
		if (propHash == null) {
			if (other.propHash != null)
				return false;
		} else if (!propHash.equals(other.propHash))
			return false;
		if (rawPropHash == null) {
			if (other.rawPropHash != null)
				return false;
		} else if (!rawPropHash.equals(other.rawPropHash))
			return false;
		return true;
	}

	@Override
	public int getRecordLength() {
		// TODO Auto-generated method stub
		return propHash.size();
	}

	@Override
	public String[] getHeaders() {
		// TODO Auto-generated method stub
		return (String[]) propHash.keySet().toArray(new String[]{});
	}

	@Override
	public Object[] getValues() {
		// TODO Auto-generated method stub
		return propHash.values().toArray();
	}
	
	@Override
	public Object[] getRawValues() {
		// TODO Auto-generated method stub
		return rawPropHash.values().toArray();
	}

	@Override
	public String toRawString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toJson() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addField(String fieldName, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getField(String fieldName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getRawHeaders() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addFields(String[] addHeaders, Object[] addValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IHeadersDataRow copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addFields(String addHeader, Object addValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public HEADERS_DATA_ROW_TYPE getHeaderType() {
		// TODO Auto-generated method stub
		return null;
	}

//	@Override
//	public boolean equals(ISelectStatement other){
//		boolean equal = false;
//		Hashtable otherRPropHash = other.getRPropHash();
//		Hashtable compareRPropToHash = this.getPropHash();
//		Hashtable otherPropHash = other.getRPropHash();
//		Hashtable comparePropToHash = this.getPropHash();
//		if(otherRPropHash.equals(compareRPropToHash) && otherPropHash.equals(comparePropToHash)){
//			equal = true;
//		} else {
//			System.out.println("not equal!");
//		}
//		return equal;
//	}
//	
//	@Override
//	public int hashCode(){
//		return 0;
//	}

}
