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

import java.awt.GridBagConstraints;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.weka.WekaClassification;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

public class WekaClassificationPlaySheet extends DendrogramPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaClassificationPlaySheet.class.getName());
	
	private String modelName;
	private WekaClassification alg = null;
	private int classColumn = -1;
	
	private List<String> skipAttributes;

	public WekaClassificationPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}
	
	@Override
	public void runAnalytics() {
		alg = new WekaClassification();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), modelName); // default of 0 is acceptable
		if(classColumn == -1) {
			classColumn = dataFrame.getNumCols() - 1;
		} 
		selectedOptions.put(options.get(1).getName(), classColumn);
		selectedOptions.put(options.get(2).getName(), skipAttributes);
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);
		alg.processTreeString();
	}
	
	@Override
	public void processQueryData() {
		Map<String, Map> rootMap = alg.getTreeMap();
		
		System.out.println(rootMap);
		HashSet hashSet = new HashSet();
		
		processTree(rootMap, hashSet);

		String root = engine.getEngineName();
		
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("name", root);
		allHash.put("children", hashSet);
		
		DecimalFormat df = new DecimalFormat("#%");
		ArrayList<Hashtable<String, Object>> statList = new ArrayList<Hashtable<String, Object>>();
		Hashtable<String, Object> statHash = new Hashtable<String, Object>();
		statHash.put("Accuracy", df.format(alg.getAccuracy()/100));
		statList.add(statHash);
		statHash = new Hashtable<String, Object>();
		statHash.put("Precision", df.format(alg.getPrecision()/100));
		statList.add(statHash);
		allHash.put("stats", statList);
		
		this.dataHash= allHash ;
	}
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		if (querySplit.length == 1) {
			this.query = query;
			this.modelName = "J48";
		} else {
			this.query = querySplit[0];
			this.modelName = querySplit[1].trim();
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
	
	public void setSkipAttributes(List<String> skipColumns) {
		this.skipAttributes = skipColumns;
	}
	
	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". Classification");
		}
	}

	public void addScrollPanel(JPanel panel, JComponent obj) {
		JScrollPane scrollPane = new JScrollPane(obj);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		panel.add(scrollPane, gbc_scrollPane);
	}

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
}
