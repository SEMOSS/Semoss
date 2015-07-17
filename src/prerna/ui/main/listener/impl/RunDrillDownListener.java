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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

/**
 * Runs the algorithm selected on the Cluster/Classify playsheet and adds additional tabs. Tied to the button to the ClassifyClusterPlaySheet.
 */
public class RunDrillDownListener extends AbstractListener {
	private static final Logger LOGGER = LogManager.getLogger(RunDrillDownListener.class.getName());
	
	private ClassifyClusterPlaySheet playSheet;
	
	private ITableDataFrame dataFrame;
	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private ArrayList<JCheckBox> columnCheckboxes;

	private String clusterIDCol;
	private int clusterIDIndex;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		//get the tabName selected
		//use tabName to get the playSheetSelected
		String tabName = drillDownTabSelectorComboBox.getSelectedItem() + "";
		ClusteringVizPlaySheet clusteringPlaySheet = (ClusteringVizPlaySheet) playSheetHash.get(tabName);
		this.dataFrame = clusteringPlaySheet.getDataFrame();
		this.clusterIDCol = clusteringPlaySheet.getClusterIDCol();
		this.clusterIDIndex = clusteringPlaySheet.getClusterIDIndex();
		
		String[] columnHeaders = dataFrame.getColumnHeaders();
//		List<String> skipColumns = new ArrayList<String>();
//		for(int i = 0; i < columnCheckboxes.size(); i++) {
//			if(columnCheckboxes.get(i).isSelected()) {
//				skipColumns.add(columnHeaders[i+1]);
//			}
//		}
		
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
		Iterator<List<Object[]>> it = dataFrame.uniqueIterator(clusterIDCol, false, null);
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
		
		ITableDataFrame newDataFrame = new BTreeDataFrame(newColNames);
		for(int i = 0; i < subsetValues.size(); i++) {
			Map<String, Object> hashRow = new HashMap<String, Object>();
			Object[] row = subsetValues.get(i);
			for(int j = 0; j < columnHeaders.length; j++) {
				if(j != clusterIDIndex) {
					hashRow.put(columnHeaders[j], row[j]);
				}
			}
			newDataFrame.addRow(hashRow, hashRow);
		}
		
		ClassifyClusterPlaySheet drillDownPlaySheet = new ClassifyClusterPlaySheet();
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+playSheet.getTitle();
		QuestionPlaySheetStore.getInstance().put(insightID,  drillDownPlaySheet);
		drillDownPlaySheet.setQuery(playSheet.getQuery());
		drillDownPlaySheet.setRDFEngine(playSheet.engine);
		drillDownPlaySheet.setQuestionID(insightID);
		drillDownPlaySheet.setTitle(playSheet.getTitle());
		drillDownPlaySheet.setDataFrame(newDataFrame);
		
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		drillDownPlaySheet.setJDesktopPane(pane);
		
		PlaysheetCreateRunner playRunner = new PlaysheetCreateRunner(drillDownPlaySheet);
		Thread playThread = new Thread(playRunner);
		playThread.start();
		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (ClassifyClusterPlaySheet)view;
		this.columnCheckboxes = playSheet.getColumnCheckboxes();
		this.drillDownTabSelectorComboBox = playSheet.getDrillDownTabSelectorComboBox();
		this.playSheetHash = playSheet.getPlaySheetHash();
	}
}
