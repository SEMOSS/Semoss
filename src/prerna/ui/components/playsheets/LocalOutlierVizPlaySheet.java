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
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.clustering.LocalOutlierFactorAlgorithm;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class LocalOutlierVizPlaySheet extends BrowserPlaySheet {
	
	private static final Logger LOGGER = LogManager.getLogger(LocalOutlierVizPlaySheet.class.getName());
	private ArrayList<Object[]> masterList;
	private String[] masterNames;
	private double[] lop;
	private int k = 25;
	
	/**
	 * Constructor for LocalOutlierVizPlaySheet. TODO needs to be changed to correct playsheet name when created
	 */
	public LocalOutlierVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatter-plot-matrix.html";
	}
	
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
		lop = alg.getLop();
	}
	
	@Override
	public Hashtable processQueryData() {
		runAnalytics(); // TODO: remove this once playsheet stop sucking
		int i;
		int j;
		int numInstances = list.size();
		int numCols = names.length;
		int newNumCols = numCols + 2;
		
		List<List<Object>> retItemList = new ArrayList<List<Object>>();
		for (i = 0; i < numInstances; i++) {
			Object[] instanceRow = list.get(i);
			List<Object> item = new ArrayList<Object>();
			
			// count is first item
			// outlier lop is second item
			// then all the variables from analysis
			item.add(1);
			
			if (Double.isNaN(lop[i])) {
				item.add("NaN");
			} else {
				item.add(lop[i]);
			}
			
			for (j = 0; j < numCols; j++)
				item.add(instanceRow[j]);
			
			retItemList.add(item);
		}
		
		String[] headers = new String[newNumCols];
		headers[0] = "Count";
		headers[1] = "LOP";
		for (i = 0; i < numCols; i++) {
			headers[i + 2] = names[i];
		}
		
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();
		retHash.put("headers", headers);
		retHash.put("dataSeries", retItemList);
		
		return retHash;
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
			addPanelAsTab(count + ". Outliers");
		}
		
		new CSSApplication(getContentPane());
	}
	
	public void setMasterList(ArrayList<Object[]> masterList) {
		this.masterList = masterList;
	}
	
	public void setMasterNames(String[] masterNames) {
		this.masterNames = masterNames;
	}
	
	public void setKNeighbors(int k) {
		this.k = k;
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
	
	public double[] getLop() {
		return this.lop;
	}
}
