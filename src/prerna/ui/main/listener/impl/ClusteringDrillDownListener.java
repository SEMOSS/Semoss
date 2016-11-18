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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class ClusteringDrillDownListener extends AbstractListener {
	
	private ClusteringVizPlaySheet playSheet;
	private ITableDataFrame dataFrame;
	private ArrayList<JCheckBox> paramCheckboxes;
	private String clusterIDCol;
	private int clusterIDIndex;
	private int inputNumClusters;
	
	/**
	 * Updates the parameters to cluster on based on the params selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		String[] columnHeaders = dataFrame.getColumnHeaders();
		
		List<String> skipColumns = new ArrayList<String>();
		for(int i = 0; i < paramCheckboxes.size(); i++) {
			if(!paramCheckboxes.get(i).isSelected()) {
				skipColumns.add(columnHeaders[i+1]);
			}
		}
		
		ArrayList<JCheckBox> clusterCheckboxes = playSheet.getClusterCheckboxes();
		List<Integer> clustersToInclude = new ArrayList<Integer>();
		for(int i = 0; i < clusterCheckboxes.size(); i++) {
			JCheckBox checkbox = clusterCheckboxes.get(i);
			if(checkbox.isSelected()) {
				clustersToInclude.add(Integer.parseInt(checkbox.getText()));
			}
		}
		
		if(clustersToInclude.isEmpty()) {
			Utility.showError("No clusters were selected to drill down on. Please select at least one and retry.");
			return;
		}
		
		List<Object[]> subsetValues = new ArrayList<Object[]>();
		Iterator<List<Object[]>> it = dataFrame.uniqueIterator(clusterIDCol);
		while(it.hasNext()) {
			List<Object[]> clusterData = it.next();
			int clusterNumber = (int) clusterData.get(0)[clusterIDIndex];
			
			if(clustersToInclude.contains(clusterNumber)) {
				subsetValues.addAll(clusterData);
			}
		}
		
		// colNames without the previous cluster index
		String[] newColNames = new String[columnHeaders.length - 1];
		int counter = 0;
		for(int i = 0; i < columnHeaders.length; i++) {
			if(i != clusterIDIndex) {
				newColNames[counter] = columnHeaders[i];
				counter++;
			}
		}
		
		ITableDataFrame newDataFrame = new H2Frame(newColNames);
		for(int i = 0; i < subsetValues.size(); i++) {
			Map<String, Object> hashRow = new HashMap<String, Object>();
			Object[] row = subsetValues.get(i);
			for(int j = 0; j < columnHeaders.length; j++) {
				if(j != clusterIDIndex) {
					hashRow.put(columnHeaders[j], row[j]);
				}
			}
			newDataFrame.addRow(hashRow);
		}
		
		ClusteringVizPlaySheet drillDownPlaySheet = new ClusteringVizPlaySheet();
		drillDownPlaySheet.setJDesktopPane(playSheet.pane);
		Insight in = new Insight(drillDownPlaySheet);
		InsightStore.getInstance().put(in);
		drillDownPlaySheet.setQuery(playSheet.getQuery());
		drillDownPlaySheet.setRDFEngine(playSheet.engine);
		drillDownPlaySheet.setTitle(playSheet.getTitle());
		drillDownPlaySheet.setSelectedParams(paramCheckboxes);
		drillDownPlaySheet.drillDownData(newDataFrame, skipColumns, inputNumClusters);
		
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		drillDownPlaySheet.setJDesktopPane(pane);
		Runnable playRunner = new PlaysheetCreateRunner(drillDownPlaySheet);	
		Thread playThread = new Thread(playRunner);
		playThread.start();
		
	}
	public void setPlaySheet(ClusteringVizPlaySheet playSheet) {
		this.playSheet = playSheet;
	}
		
	public void setCheckBoxes(ArrayList<JCheckBox> paramCheckboxes) {
		this.paramCheckboxes = paramCheckboxes;
	}

	public void setDataFrame(ITableDataFrame dataFrame) {
		this.dataFrame = dataFrame;
	}
	
	public void setClusterIDCol(String clusterIDCol) {
		this.clusterIDCol = clusterIDCol;
	}

	public void setClusterIDIndex(int clusterIDIndex) {
		this.clusterIDIndex = clusterIDIndex;
	}
	
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
	public void setNumClusters(int inputNumClusters) {
		this.inputNumClusters = inputNumClusters;
	}
	
}
