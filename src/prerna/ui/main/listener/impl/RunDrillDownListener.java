/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JProgressBar;
import javax.swing.JTabbedPane;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.components.playsheets.WekaClassificationPlaySheet;
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
	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private ArrayList<JCheckBox> columnCheckboxes;
	
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
		String[] masterNames = clusteringPlaySheet.getMasterNames();
		ArrayList<Object []> masterList = clusteringPlaySheet.getMasterList();
		
		Integer numberSelected = 0;
		for(int i=0;i<columnCheckboxes.size();i++) {
			if(columnCheckboxes.get(i).isSelected())
				numberSelected++;
		}
		
		String[] filteredNames = new String[numberSelected+1];
		filteredNames[0] = masterNames[0];
		int colInd=1;
		for(int i=0;i<columnCheckboxes.size();i++) {
			if(columnCheckboxes.get(i).isSelected()) {
				filteredNames[colInd] = masterNames[1+i];
				colInd++;
			}
		}
		
		ArrayList<JCheckBox> clusterCheckboxes = playSheet.getClusterCheckboxes();
		ArrayList<Integer> clustersToInclude = new ArrayList<Integer>();
		for(int i=0;i<clusterCheckboxes.size();i++) {
			JCheckBox checkbox = clusterCheckboxes.get(i);
			if(checkbox.isSelected()) {
				clustersToInclude.add(Integer.parseInt(checkbox.getText()));
			}
		}
		
		Hashtable<String, Integer> instanceIndexHash = clusteringPlaySheet.getInstanceIndexHash();
		int[] clusterAssigned  = clusteringPlaySheet.getClusterAssigned();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(Object[] instanceRow : masterList) {
			String instance = (String)instanceRow[0];
			int cluster = clusterAssigned[instanceIndexHash.get(instance)];
			//check if cluster is to be included
			if(clustersToInclude.contains(cluster)) {
				newList.add(instanceRow);
			}
		}
		
		//take out the clusters we dont care about from the master list...
		//then do the filtering after that.
		ArrayList<Object[]> filteredList = new ArrayList<Object []>();
		for(Object[] row : newList) {
			Object[] filteredRow = new Object[numberSelected+1];
			filteredRow[0] = row[0];//whatever object name we're clustering on
			colInd=1;
			for(int i=0;i<columnCheckboxes.size();i++) {
				if(columnCheckboxes.get(i).isSelected()) {
					filteredRow[colInd] = row[1+i];
					colInd++;
				}
			}
			filteredList.add(filteredRow);
		}
		
		ClassifyClusterPlaySheet drillDownPlaySheet = new ClassifyClusterPlaySheet();
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+playSheet.getTitle();
		QuestionPlaySheetStore.getInstance().put(insightID,  drillDownPlaySheet);
		drillDownPlaySheet.setQuery(playSheet.getQuery());
		drillDownPlaySheet.setRDFEngine(playSheet.engine);
		drillDownPlaySheet.setQuestionID(insightID);
		drillDownPlaySheet.setTitle(playSheet.getTitle());
		drillDownPlaySheet.setData(masterNames, newList);
		
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		drillDownPlaySheet.setJDesktopPane(pane);
		
		drillDownPlaySheet.runAnalytics();
		drillDownPlaySheet.createView();
		drillDownPlaySheet.setSelectedColumns(filteredNames);
		
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
