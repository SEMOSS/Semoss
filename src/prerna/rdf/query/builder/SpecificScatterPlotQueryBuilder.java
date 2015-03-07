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

public class SpecificScatterPlotQueryBuilder extends AbstractSpecificQueryBuilder{
	static final Logger logger = LogManager.getLogger(SpecificScatterPlotQueryBuilder.class.getName());

	String labelColName;
	String xAxisColName;
	String yAxisColName;
	String zAxisColName;
	String xAxisMathFunc;
	String yAxisMathFunc;
	String zAxisMathFunc;
	String seriesColName;
	
	public SpecificScatterPlotQueryBuilder(String labelColName, String xAxisColName, String yAxisColName, String zAxisColName, String xAxisMathFunc, String yAxisMathFunc, String zAxisMathFunc, String seriesColName, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.labelColName = labelColName;
		this.xAxisColName = xAxisColName;
		this.yAxisColName = yAxisColName;
		this.zAxisColName = zAxisColName;
		this.xAxisMathFunc = xAxisMathFunc;
		this.yAxisMathFunc = yAxisMathFunc;
		this.zAxisMathFunc = zAxisMathFunc;
		this.seriesColName = seriesColName;
	}
	
    public void buildQuery () {
        ArrayList<String> varNames = uniqifyColNames(Arrays.asList( labelColName, xAxisColName, yAxisColName, zAxisColName, seriesColName ));
        ArrayList<String> groupList = new ArrayList<String>();

        // the order for the return variables is series, label, x, y, z
        // start with series
        if(seriesColName != null){
        	logger.info("Adding series: " + seriesColName);
        	addReturnVariable(seriesColName, varNames.get(4), baseQuery, "false");
            groupList.add(seriesColName);
        }
        
        //label
        logger.info("Adding label: " + labelColName);
        addReturnVariable(labelColName, varNames.get(0), baseQuery, "false");
        groupList.add(labelColName);


        //x
        logger.info("Adding x-axis variable: " + xAxisColName);
        addReturnVariable(xAxisColName, varNames.get(1), baseQuery, xAxisMathFunc);
        
        //y
        logger.info("Adding y-axis variable: " + yAxisColName);
        addReturnVariable(yAxisColName, varNames.get(2), baseQuery, yAxisMathFunc);
        
        //z if it is not null
        if(zAxisColName != null){
        	logger.info("Adding z-axis variable: " + zAxisColName);
        	addReturnVariable(zAxisColName, varNames.get(3), baseQuery, zAxisMathFunc);        	
        }

        //add them as group by
        if(!groupList.isEmpty()){
        	logger.info("Adding GroupBy to query: " + groupList);
        	SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
        }
        
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
        
        createQuery();
        logger.info("Created ScatterPlot Query: " + query);
    }

	@Override
	public void buildQueryR() {
		// TODO Auto-generated method stub
		
	}

}
