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
import java.util.ArrayList;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.supervized.CorrelationAlgorithm;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

public class CorrelationPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(CorrelationPlaySheet.class.getName());
	
	private int numVariables;
	private String[] columnHeaders;
	private double[][] correlation;
	private List<String> skipInstances;
	
	protected JTabbedPane jTab;
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty()) {
			super.createData();
		}
	}

	@Override
	public void runAnalytics() {
		columnHeaders = dataFrame.getColumnHeaders();
		numVariables = columnHeaders.length - 1;
		//calculate the correlation for each pair of columns
		CorrelationAlgorithm alg = new CorrelationAlgorithm();
		dataFrame.performAction(alg);
		correlation = alg.getCorrelation();
	}
	
	@Override
	public List<Object[]> getTabularData() {
		List<Object[]> output = new ArrayList<Object []>();
		int i;
		int j;
		for(i = 0; i < numVariables; i++) {
			Object[] row = new Object[numVariables+1];
			row[0] = columnHeaders[i+1];
			for(j = 0; j < numVariables; j++) {
				row[j+1] = correlation[i][j];
			}
			output.add(row);
		}
		
		return output;
	}
	
	@Override
	public String[] getNames() {
		columnHeaders[0] = "-";
		return columnHeaders;
	}
	
	public void setSkipAttributes(List<String> skipInstances) {
		this.skipInstances = skipInstances;
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
			addPanelAsTab(count + ". Corretaion Raw Data");
		}
	}

	public void addPanelAsTab(String tabName) {
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
