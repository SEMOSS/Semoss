/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class HeatMapQueryBuilder extends AbstractQueryBuilder {
	static final Logger logger = LogManager.getLogger(HeatMapQueryBuilder.class.getName());
	String xAxisColName;
	String yAxisColName;
	String heatName;
	String heatMathFunc;
	
	public HeatMapQueryBuilder(String xAxisColName, String yAxisColName, String heatName, String heatMathFunc, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		
		this.xAxisColName = xAxisColName;
		this.yAxisColName = yAxisColName;
		this.heatName = heatName;
		this.heatMathFunc = heatMathFunc;
	}
	
	@Override
	public void buildQuery () {
		ArrayList<String> varNames = uniqifyColNames(Arrays.asList( xAxisColName, yAxisColName, heatName ));
		ArrayList<String> groupList = new ArrayList<String>();

		//add x axis to query
		logger.info("Adding X-Axis: " + xAxisColName + " with name: " + varNames.get(0));
		addReturnVariable(xAxisColName, varNames.get(0), baseQuery, "false");
		groupList.add(xAxisColName);

		//add y axis to query
		logger.info("Adding Y-Axis: " + yAxisColName + " with name: " + varNames.get(1));
		addReturnVariable(yAxisColName, varNames.get(1), baseQuery, "false");
		groupList.add(yAxisColName);

		logger.info("Adding heat math function " + heatMathFunc + " on column " + heatName);
		addReturnVariable(heatName, varNames.get(2), baseQuery, heatMathFunc);//SEMOSSQueryHelper.addMathFuncToQuery(heatMathFunc, heatName, baseQuery, varNames.get(2));
		/*if(heatMathFunc.equals("false")){
			groupList.add(heatName);
		}*/
		//add them as group by
		logger.info("Query will GROUPBY: " + groupList);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created HeatMap Query: " + query);		
	}
}
