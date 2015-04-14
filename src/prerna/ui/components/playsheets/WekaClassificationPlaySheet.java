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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.weka.WekaClassification;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Utility;

public class WekaClassificationPlaySheet extends DendrogramPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaClassificationPlaySheet.class.getName());
	
	private String modelName;
	private WekaClassification alg = null;
	private int classColumn = -1;
	
	public WekaClassificationPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
		dataHash = processQueryData();
	}
	
	public void runAlgorithm() {
		if(classColumn<0) {
			classColumn = names.length-1;
			alg = new WekaClassification(list, names, modelName, names.length - 1);
		} else {
			LOGGER.info("Creating classifier to predict column "+names[classColumn]);
		}
		alg = new WekaClassification(list, names, modelName, classColumn);
		try {
			alg.execute();
			alg.processTreeString();
			System.out.println(alg.getTreeAsString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Hashtable processQueryData() {
		Map<String, Map> rootMap = alg.getTreeMap();
		
		System.out.println(rootMap);
		HashSet hashSet = new HashSet();
		
		processTree(rootMap, hashSet);

		String root = engine.getEngineName();
		
		dataHash.put("name", root);
		dataHash.put("children", hashSet);
		
		DecimalFormat df = new DecimalFormat("#%");
		ArrayList<Hashtable<String, Object>> statList = new ArrayList<Hashtable<String, Object>>();
		Hashtable<String, Object> statHash = new Hashtable<String, Object>();
		statHash.put("Accuracy", df.format(alg.getAccuracy()/100));
		statList.add(statHash);
		statHash = new Hashtable<String, Object>();
		statHash.put("Precision", df.format(alg.getPrecision()/100));
		statList.add(statHash);
		dataHash.put("stats", statList);
		
		return dataHash;
	}
	
	private void generateData() {
		if(query!=null) {
			list = new ArrayList<Object[]>();
			
			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			names = sjsw.getVariables();
			int length = names.length;
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				Object[] row = new Object[length];
				int i = 0;
				for(; i < length; i++) {
					row[i] = sjss.getVar(names[i]);
				}
				list.add(row);
			}
		}
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Classification");
		}
	}
	
	@Override
	public void setQuery(String query) {
		if(query.contains("\\+\\+\\+")) {
			String[] querySplit = query.split("\\+\\+\\+");
			this.query = querySplit[0];
			this.modelName = querySplit[1].trim();
		} else {
			this.query = query;
			this.modelName = "J48";
		}
	}
	
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
	
	public void setClassColumn(int classColumn){
		this.classColumn = classColumn;
	}
	
	public int getClassColumn() {
		return classColumn;
	}
}
