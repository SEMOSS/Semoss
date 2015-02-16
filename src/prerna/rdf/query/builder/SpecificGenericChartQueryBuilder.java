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
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class SpecificGenericChartQueryBuilder extends AbstractSpecificQueryBuilder {
	static final Logger logger = LogManager.getLogger(SpecificGenericChartQueryBuilder.class.getName());
	String labelColName;
	ArrayList<String> valueColNames;
	ArrayList<String> valueMathFunctions;
	
	public SpecificGenericChartQueryBuilder (String labelColName, ArrayList<String> valueColNames, ArrayList<String> valueMathFunctions, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.labelColName = labelColName;
		this.valueColNames = valueColNames;
		this.valueMathFunctions = valueMathFunctions;
	}
	
	@Override
	public void buildQuery () {
		ArrayList<String> allColNames = new ArrayList<String>();
		ArrayList<String> groupList = new ArrayList<String>();
		allColNames.add(labelColName);
		allColNames.addAll(valueColNames);
		
		ArrayList<String> allVarNames = uniqifyColNames(allColNames );
		logger.info("Unique variables are: " + allVarNames);
		
		//first add the label as return var to query
		logger.info("Adding Return Variable: " + labelColName);
		addReturnVariable(labelColName, allVarNames.get(0), baseQuery, "false");
		groupList.add(labelColName);
		
		//now iterate through to all series values with their math functions
		for(int seriesIdx = 0 ; seriesIdx < valueColNames.size(); seriesIdx++){
			String mathFunc = valueMathFunctions.get(seriesIdx);
			String seriesColName = valueColNames.get(seriesIdx);

			logger.info("Adding Return Variable: " + seriesColName);
			addReturnVariable(seriesColName, allVarNames.get(seriesIdx+1), baseQuery, mathFunc);
			
			/*if(mathFunc.equals("false")){
				groupList.add(seriesColName);
			}*/
		}

		logger.info("Adding GROUPBY to query: " + allColNames);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created BarChart Query: " + query);		
	}
}
