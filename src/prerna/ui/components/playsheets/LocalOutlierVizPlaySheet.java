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
import java.awt.GridBagConstraints;
import java.util.HashMap;
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

import prerna.algorithm.learning.unsupervised.outliers.LOF;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class LocalOutlierVizPlaySheet extends BrowserPlaySheet {
	
	private static final Logger LOGGER = LogManager.getLogger(LocalOutlierPlaySheet.class.getName());

	private LOF alg;
	private int instanceIndex;
	private int k;
	private List<String> skipAttributes;

	/**
	 * Constructor for LocalOutlierVizPlaySheet. TODO needs to be changed to correct playsheet name when created
	 */
	public LocalOutlierVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singleaxisbubblechart.html";
	}
	
	@Override
	public void createData() {
		if (dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		alg = new LOF();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), instanceIndex); // default of 0 is acceptable
		if(k == 0) {
			k = 25;
		} 
		selectedOptions.put(options.get(1).getName(), k);
		selectedOptions.put(options.get(2).getName(), skipAttributes);

		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);
	}
	
	@Override
	public void processQueryData() {
		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		String[] headers = dataFrame.getColumnHeaders();
		dataHash.put("headers", headers);
		dataHash.put("data", dataFrame.getData());
		dataHash.put("changedColIndex", ArrayUtilityMethods.calculateIndexOfArray(headers, alg.getChangedColumns().get(0)));
		this.dataHash = dataHash;
	}

	@Override
	public void setQuery(String query) {
		if (query.matches(".*\\+\\+\\+[0-9]+")) {
			String[] querySplit = query.split("\\+\\+\\+");
			this.query = querySplit[0];
			this.k = Integer.parseInt(querySplit[1].trim());
		} else {
			this.query = query;
		}
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
	}
	
	@Override
	public Hashtable getData() {
		//TODO: remove this from getData() to call the super method
		dataHash.put("id", this.questionNum==null? "": this.questionNum);
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		dataHash.put("playsheet", className);
		dataHash.put("title", this.title==null? "": this.title);
		
		Hashtable<String, String> specificData = new Hashtable<String, String>();
		specificData.put("x-axis", "LOP");
		specificData.put("z-axis", "COUNT");
		dataHash.put("specificData", specificData);
		return dataHash;
	}
	
	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
			addGridTab("Outliers Raw Data");
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". Outliers Viz Data");
			addGridTab(count + ". Outliers Raw Data");
		}
	}
	
	public void addGridTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(dataFrame.getColumnHeaders(), dataFrame.getData());
		gsp.addHorizontalScroll();
		jTab.addTab(tabName, gsp);
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
