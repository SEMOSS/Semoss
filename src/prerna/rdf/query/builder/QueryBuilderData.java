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
package prerna.rdf.query.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Utility;

public class QueryBuilderData {
	
	static final Logger LOGGER = LogManager.getLogger(QueryBuilderData.class.getName());
	
	List<List<String>> relTriples;						// List of physical name triples
	List<Map<String, String>> nodeProps;				// List of selected property objects. Each object contains SubjectVar-->Logical Name of node, uriKey-->Physical uri for property, varKey-->logical name of property
	Map<String, List<Object>> filterData;				// For each var to filter, has Logical name-->List of values to include
	List<String> varReturnOrder;						// Sets the return order for the variables
	
	private final String QUERY_DATA_KEY = "QueryData";
	private final String REL_TRIPLES_KEY = "relTriples";
	private final String FILTERS_KEY = "filter";
	private final String NODE_PROPS_KEY = "SelectedNodeProps";
	
	public QueryBuilderData(Map json){
		LOGGER.info("Instantiating QueryBuilderData with json " + json);
		
		if(json.containsKey(NODE_PROPS_KEY)){
			setNodeProps((List) json.get(NODE_PROPS_KEY));
			if(nodeProps != null && !nodeProps.isEmpty()) nodeProps.get(0).remove("db");
		}
		
		if(json.containsKey(QUERY_DATA_KEY)){
			Map queryData = (Map) json.get(QUERY_DATA_KEY);
			setRelTriples((List) queryData.get(REL_TRIPLES_KEY));
			if(queryData.get(FILTERS_KEY) != null)
				setFilterData((Map) queryData.get(FILTERS_KEY));
		} else if(json.containsKey(REL_TRIPLES_KEY)) {
			setRelTriples((List) json.get(REL_TRIPLES_KEY));
		}
 	}
	
	public void setVarReturnOrder(List<String> ordered){
		LOGGER.info("setting var return order to " + ordered.toString());
		this.varReturnOrder = ordered;
	}
	
	protected List<String> getVarReturnOrder(){
		return this.varReturnOrder;
	}
	
	public Map<String, List<Object>> getFilterData() {
		return filterData;
	}

	public void setFilterData(Map<String, List<Object>> filterData) {
		this.filterData = filterData;
	}

	public List<List<String>> getRelTriples() {
		return relTriples;
	}
	
	private void setRelTriples(List<List<String>> relTriples) {
		this.relTriples = relTriples;
	}
	
	public List<Map<String, String>> getNodeProps() {
		return nodeProps;
	}
	
	private void setNodeProps(List<Map<String, String>> nodeProps) {
		this.nodeProps = nodeProps;
	}
}
