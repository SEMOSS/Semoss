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
package prerna.ui.components.playsheets;

import java.awt.GridBagConstraints;
import java.util.HashMap;
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

import prerna.algorithm.learning.moa.MOAPerceptronRunner;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */


@SuppressWarnings("serial")
public class PerceptronPlaySheet extends GridPlaySheet {

	public double[][] weights;
	private String className;
	private List<String> skipAttributes;
	protected JTabbedPane jTab;
	private String kernelType;
	private int degree;
	private double kappa;
	private double constant;

	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	private static final Logger logger = LogManager.getLogger(PerceptronPlaySheet.class.getName());

	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		MOAPerceptronRunner alg = new MOAPerceptronRunner();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();

		if(className.equals("")) {
			String[] colNames = dataFrame.getColumnHeaders();
			className = colNames[colNames.length - 1];
		} 

		selectedOptions.put(options.get(0).getName(), className);
		selectedOptions.put(options.get(1).getName(), skipAttributes);
		selectedOptions.put(options.get(2).getName(), kernelType);
		selectedOptions.put(options.get(3).getName(), degree);
		selectedOptions.put(options.get(4).getName(), kappa);
		selectedOptions.put(options.get(5).getName(), constant);
		
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);
	}

	@Override
	public void processQueryData() {

	}

	public void setWeights(double[][] w) {
		weights = w;
	}

	public void setClassColumn(String className){
		this.className = className;
	}

	public String getClassColumn() {
		return className;
	}

	public void setSkipAttributes(List<String> skipColumns) {
		this.skipAttributes = skipColumns;
	}
	
	public void setDegree(int degree) {
		this.degree = degree;
	}
	
	public void setKernel(String kernel) {
		this.kernelType = kernel;
	}


	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			//			addPanelAsTab(count + ". Perceptron Viz Data");
			addGridTab(count + ". Perceptron Raw Data");
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
