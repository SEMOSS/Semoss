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
package prerna.om;

import java.util.StringTokenizer;
import java.util.Vector;

public class SEMOSSParam {

	String name = null;
	String query = null;
	String type = null;
	Vector<String> options = new Vector<String>();
	Boolean hasQuery = true;
	String uri = null;
	
	public void setUri(String uri){
		this.uri = uri;
	}
	
	public String getUri(){
		return this.uri;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type.replace("\"","").trim();
	}

	String depends = "false";
	Vector<String> dependVars = new Vector<String>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name.replace("\"","").trim();
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query.replace("\"","").trim();
		this.hasQuery = true;
	}
	public String isDepends() {
		return depends;
	}
	public void setDepends(String depends) {
		this.depends = depends.replace("\"","").trim();
	}
	public void addDependVar(String dependVar)
	{
		dependVars.addElement(dependVar.replace("\"","").trim());
	}
	
	public Vector<String> getDependVars()
	{
		return this.dependVars;
	}
	
	public void setOptions(String optionString) {
		optionString = optionString.replaceAll("\"", "");
		StringTokenizer st = new StringTokenizer(optionString, ";");
		while(st.hasMoreElements()) {
			options.add((String)st.nextElement());
		}
		this.hasQuery=false;
	}
	public Vector<String> getOptions() {
		return options;
	}
	public Boolean isQuery() {
		return hasQuery;
	}
	
}
