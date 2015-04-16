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

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.clustering.LocalOutlierFactorAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

public class LocalOutlierPlaySheet extends GridPlaySheet {

	private ArrayList<Object[]> masterList;
	private String[] masterNames;

	private double[] lop;
	private double[] lrd;
	private double[] lof;

	private static final Logger LOGGER = LogManager.getLogger(LocalOutlierPlaySheet.class.getName());
	protected JTabbedPane jTab;
	private int k;

	@Override
	public void createData() {
		if (list == null || list.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		LocalOutlierFactorAlgorithm alg = new LocalOutlierFactorAlgorithm(list, names);
		alg.setK(k);
		alg.execute();

		list = alg.getMasterTable();
		names = alg.getNames();

		lrd = alg.getLrd();
		lof = alg.getLof();
		// double[] zScore = alg.getZScore();
		lop = alg.getLop();

		if (masterNames != null) {
			// asterisk the columns that have not been included in analysis
			String[] asteriskMasterNames = new String[masterNames.length];
			int i;
			int j;
			for (i = 0; i < masterNames.length; i++) {
				String name = masterNames[i];
				Boolean inFilteredSet = false;
				for (j = 0; j < names.length; j++) {
					// see if the column is used in the filtered set
					// if not then asterisk
					if (names[j].equals(name)) {
						inFilteredSet = true;
					}
				}
				if (!inFilteredSet)
					name = "*" + name;
				asteriskMasterNames[i] = name;
			}
			list = masterList;
			names = asteriskMasterNames;
		}

		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		int numRows = list.size();
		int numCols = names.length;
		int newNumCols = numCols + 3;
		int i;
		int j;
		for (i = 0; i < numRows; i++) {
			Object[] newRow = new Object[newNumCols];
			Object[] row = list.get(i);
			for (j = 0; j <= numCols; j++) {
				if (j == numCols) {
					// newRow[j] = lrd[i];
					newRow[j] = Math.round(lrd[i] * 100) / 100.0;
					if (Double.isInfinite(lof[i])) {
						newRow[j + 1] = "Inf";
					} else {
						// newRow[j+1] = lof[i];
						newRow[j + 1] = Math.round(lof[i] * 100) / 100.0;
					}
					if (Double.isNaN(lop[i])) {
						newRow[j + 2] = "NaN";
					} else {
						// newRow[j+2] = zScore[i];
						// newRow[j+2] = Math.round(zScore[i] * 100) / 100.0;
						newRow[j + 2] = String.format("%.0f%%", lop[i] * 100);
					}
				} else {
					newRow[j] = row[j];
				}
			}
			newList.add(newRow);
		}
		list = newList;

		String[] newNames = new String[newNumCols];
		for (i = 0; i <= numCols; i++) {
			if (i == numCols) {
				newNames[i] = "LRD";
				newNames[i + 1] = "LOF";
				newNames[i + 2] = "LOP";
			} else {
				newNames[i] = names[i];
			}
		}
		names = newNames;
	}

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
			addPanelAsTab(count + ". Outliers Raw Data");
		}
	}

	public void addPanelAsTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(names, list);
		gsp.addHorizontalScroll();
		jTab.addTab(tabName, gsp);
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

	@Override
	public Object getVariable(String varName, ISelectStatement sjss) {
		return sjss.getVar(varName);
	}

	public void setMasterList(ArrayList<Object[]> masterList) {
		this.masterList = masterList;
	}

	public void setMasterNames(String[] masterNames) {
		this.masterNames = masterNames;
	}

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}

	public void setKNeighbors(int k) {
		this.k = k;
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

	public double[] getLop() {
		return lop;
	}

	public void setLop(double[] lop) {
		this.lop = lop;
	}

	public double[] getLrd() {
		return lrd;
	}

	public void setLrd(double[] lrd) {
		this.lrd = lrd;
	}

	public double[] getLof() {
		return lof;
	}

	public void setLof(double[] lof) {
		this.lof = lof;
	}
}
