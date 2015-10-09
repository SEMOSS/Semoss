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
package prerna.rdf.util;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

public abstract class AbstractQueryParser {

	protected boolean hasColumnAggregatorFunction = false;
	protected Set<String> returnVariables = new HashSet<String>();
	protected Hashtable<String,Hashtable<String,String>> typePropVariables = new Hashtable<String, Hashtable<String,String>>();
	protected Hashtable<String,Hashtable<String,String>> typeReturnVariables = new Hashtable<String, Hashtable<String,String>>();

	protected Hashtable <String, String> types = new Hashtable<String, String>();
	protected Hashtable <String, String> props = new Hashtable<String, String>();
	protected String query;
	protected List<String[]> triplesData = new ArrayList<String[]>();
	protected HashMap<String,String> aliasTableMap = new HashMap<String, String>();

	public AbstractQueryParser(){
		this.query = "";
	}
	
	public AbstractQueryParser(String query){
		this.query = query;
	}
	
	public abstract void parseQuery();
	public abstract List<String[]> getTriplesData();
	
	public void setQuery(String query){
		this.query = query;
	}
	
	public Hashtable<String, String> getNodesFromQuery(){
		return types;
	}
	
	public Hashtable<String, Hashtable<String,String>> getPropertiesFromQuery(){
		return typePropVariables;
	}
	
	public Hashtable<String, Hashtable<String,String>> getReturnVariables(){
		return typeReturnVariables;
	}
	
	protected void addToVariablesMap(Hashtable<String, Hashtable<String, String>> mappingObj, String tableName, String columnAlias, String columnName){
		if(mappingObj.get(tableName)!=null){
			Hashtable<String,String> returnVariablesForCurrentTable = mappingObj.get(tableName);
			returnVariablesForCurrentTable.put(columnAlias, columnName);
			mappingObj.put(tableName, returnVariablesForCurrentTable);
		} else {
			Hashtable<String,String> returnVariablesForCurrentTable = new Hashtable<String,String>();
			returnVariablesForCurrentTable.put(columnAlias, columnName);
			mappingObj.put(tableName, returnVariablesForCurrentTable);
		}
	}
	
	public boolean hasAggregateFunction(){
		return hasColumnAggregatorFunction;
	}

}