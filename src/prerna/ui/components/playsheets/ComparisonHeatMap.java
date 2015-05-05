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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class ComparisonHeatMap extends BrowserPlaySheet {
	
	private String[] databaseArr;
	private String[] queryArr;
	
	public ComparisonHeatMap() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/heatmap.html";
	}
	
	
	@Override
	public void createData() {
		int i = 0;
		int length = queryArr.length;
		
		Hashtable<String, Object> simValuesHash = new Hashtable<String, Object>();
		for(;i < length; i++) {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(databaseArr[i]);
			ISelectWrapper sjsw = Utility.processQuery(engine, queryArr[i]);
			Hashtable<String, List<String>> queryData = runQuery(sjsw);
			simValuesHash = runComparison(queryData, simValuesHash);
		}
		
		simValuesHash = averageSimScore(length, simValuesHash);
		
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("dataSeries", simValuesHash);
		allHash.put("title",  "Similarity Heat Map");
		allHash.put("xAxisTitle", "x");
		allHash.put("yAxisTitle", "y");
		allHash.put("value", "score");
		
		dataHash = allHash;
	}
	
	public Hashtable<String, List<String>> runQuery(ISelectWrapper sjsw) {
		String[] names = sjsw.getVariables();
		
		Hashtable<String, List<String>> dataHash = new Hashtable<String, List<String>>();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String instance = sjss.getVar(names[0]).toString();
			String value = sjss.getVar(names[1]).toString();
			
			List<String> valList;
			if(dataHash.containsKey(instance)) {
				valList = dataHash.get(instance);
				valList.add(value);
			} else {
				valList = new ArrayList<String>();
				valList.add(value);
				dataHash.put(instance, valList);
			}
		}
		
		return dataHash;
	}
	
	public Hashtable<String, Object> runComparison(Hashtable<String, List<String>> queryData, Hashtable<String, Object> simValuesHash) {
		for(String instance1 : queryData.keySet()) {
			List<String> instance1Values = queryData.get(instance1);
			
			for(String instance2: queryData.keySet()) {
				int simCount = 0;

				List<String> instance2Values = queryData.get(instance2);
				
				Set<String> uniqueValues = new HashSet<String>();
				uniqueValues.addAll(instance1Values);
				uniqueValues.addAll(instance2Values);
				int uniqueValuesCount = uniqueValues.size();
				
				for(String val : instance2Values) {
					if(instance1Values.contains(val)) {
						simCount++;
					}
				}
				
				double simScore = (double) simCount/ uniqueValuesCount;
				String key = instance1.concat("+").concat(instance2);
				if(simValuesHash.containsKey(key)) {
					@SuppressWarnings("unchecked")
					Hashtable<String, Object> elemHash = (Hashtable<String, Object>) simValuesHash.get(key);
					double oldSim = (double) elemHash.get("score");
					elemHash.put("score", simScore + oldSim);
				} else {
					Hashtable<String, Object> elemHash = new Hashtable<String, Object>();
					elemHash.put("x", instance1);
					elemHash.put("y", instance2);
					elemHash.put("score", simScore);
					simValuesHash.put(key, elemHash);
				}
			}
		}
		
		return simValuesHash;
	}
	
	private Hashtable<String, Object> averageSimScore(int length, Hashtable<String, Object> simValuesHash) {
		for(String s : simValuesHash.keySet()) {
			@SuppressWarnings("unchecked")
			Hashtable<String, Object> innerHash = (Hashtable<String, Object>) simValuesHash.get(s);
			double totalScore = (double) innerHash.get("score");
			double score = totalScore / length;
			innerHash.put("score", score);
		}
		
		return simValuesHash;
	}
	
	/**
	 * Divides the input into db names and queries
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		String[] tokens = query.split("\\+\\+\\+");
		int i = 0;
		int length = tokens.length;
		databaseArr = new String[length/2];
		queryArr = new String[length/2];
		
		int index = 0;
		for(;i < length; i+=2) {
			databaseArr[index] = tokens[i];
			queryArr[index] = tokens[i+1];
			index++;
		}
	}

}
