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
package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.SimilarityHeatMapSheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.BrowserFunction;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 * An browser class for refreshing similarity comparison heat map based on selected parameters.
 */
public class SimilarityBarChartBrowserFunction implements BrowserFunction {

	static final Logger logger = LogManager.getLogger(SimilarityBarChartBrowserFunction.class.getName());
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	final String valueString = "Score";
	final String keyString = "key";
	SimilarityHeatMapSheet simHeat = null;
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public JSValue invoke(JSValue... arg0){
		Gson gson = new Gson();

		String cellKey = arg0[0].getString();
		logger.info("cellKey = " + cellKey);
		String[] selectedVars = gson.fromJson(arg0[1].getString(), String[].class);
		Hashtable<String, Double> specifiedWeights = new Hashtable<String, Double> ();
		simHeat.getSimBarChartData(cellKey, selectedVars, specifiedWeights);
		JSValue finalJson = JSValue.create(simHeat.getSimBarChartData(cellKey, selectedVars, specifiedWeights));
		System.out.println("Java is done");
		return finalJson;
	}
	
	/**
	 * Method setParamDataHash.  Sets the hashtable and orders the keys for processing
	 * @param engine paramDataHash - The hashtable used for processing
	 */
	public void setSimHeatPlaySheet(SimilarityHeatMapSheet simHeat){
		this.simHeat = simHeat;
	}
}

