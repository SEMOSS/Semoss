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
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class SpecificPieChartQueryBuilder extends AbstractSpecificQueryBuilder {
	static final Logger logger = LogManager.getLogger(SpecificPieChartQueryBuilder.class.getName());

	String label;
	String valueName;
	String valueMathFunc;
	
	public SpecificPieChartQueryBuilder (String label, String valueName, String valueMathFunc, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.label = label;
		this.valueName = valueName;
		this.valueMathFunc = valueMathFunc;
	}
	
	@Override
	public void buildQuery () {
		ArrayList<String> varNames = uniqifyColNames(Arrays.asList(label, valueName));		
		
		//add label to query
		logger.info("Adding label: " + label + " with name: " + varNames.get(0));
		addReturnVariable(label, varNames.get(0), baseQuery, "false");
		
		// add the heat value
		logger.info("Adding value math function " + valueMathFunc + " on column " + valueName);
		addReturnVariable(valueName, varNames.get(1), baseQuery, valueMathFunc);
		
		//add them as group by
		ArrayList<String> groupList = new ArrayList<String>();
		groupList.add(label);
		logger.info("Query will GroupBy: " + groupList);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created PieChart Query: " + query);		
	}

	@Override
	public void buildQueryR() {
		// TODO Auto-generated method stub
		ArrayList<String> colAliases = uniqifyColNames(Arrays.asList(label, valueName));		

		//add label to query
		logger.info("Adding label: " + label + " with name: " + colAliases.get(0));
		//addReturnVariable(label, colAliases.get(0), baseQuery, "false");
		
		// add the heat value
		logger.info("Adding value math function " + valueMathFunc + " on column " + valueName);
		//addReturnVariable(valueName, colAliases.get(1), baseQuery, valueMathFunc);
		
		//add the as group by for the label
		paramString = generateSelectorsSubString(label, colAliases.get(0), true, "");

		//next do the values
		paramString += " , " + generateSelectorsSubString(valueName, colAliases.get(1), false, valueMathFunc);

		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createSQLQuery();
		logger.info("Created PieChart Query: " + query);		
		
	}

}
