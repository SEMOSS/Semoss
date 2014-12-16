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
import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.helpers.PlaysheetOverlayRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class ClusteringDrillDownListener extends AbstractListener {
	
	//need to pass the checkboxes, the names, and the master data list,
	//filters master data list to only include elements that were in the clusters marked in the checkbox list
	
	private ArrayList<JCheckBox> paramCheckboxes;
	private String[] masterNames;
	private ArrayList<Object []> masterList;
	private ClusteringVizPlaySheet playSheet;
	
	/**
	 * Updates the parameters to cluster on based on the params selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		
		int i;
		Integer numberSelected = 0;
		for(i = 0; i < paramCheckboxes.size(); i++) {
			if(paramCheckboxes.get(i).isSelected())
				numberSelected++;
		}
		
		String[] filteredNames = new String[numberSelected+1];
		filteredNames[0] = masterNames[0];
		int colInd=1;
		for(i = 0; i < paramCheckboxes.size(); i++) {
			if(paramCheckboxes.get(i).isSelected()) {
				filteredNames[colInd] = masterNames[1+i];
				colInd++;
			}
		}
		
		ArrayList<JCheckBox> clusterCheckboxes = playSheet.getClusterCheckboxes();
		ArrayList<Integer> clustersToInclude = new ArrayList<Integer>();
		for(i = 0; i < clusterCheckboxes.size(); i++) {
			JCheckBox checkbox = clusterCheckboxes.get(i);
			if(checkbox.isSelected()) {
				clustersToInclude.add(Integer.parseInt(checkbox.getText()));
			}
		}
		
		if(clustersToInclude.isEmpty()) {
			Utility.showError("No clusters were selected to drill down on. Please select at least one and retry.");
			return;
		}
		
		//TODO:delete
//		Hashtable<String, Integer> instanceIndexHash = playSheet.getInstanceIndexHash();
		int[] clusterAssigned  = playSheet.getClusterAssignment();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(i = 0; i < clusterAssigned.length; i++) {
			Object[] instanceRow = masterList.get(i);
			int cluster = clusterAssigned[i];
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
			for(i = 0; i < paramCheckboxes.size(); i++) {
				if(paramCheckboxes.get(i).isSelected()) {
					filteredRow[colInd] = row[1+i];
					colInd++;
				}
			}
			filteredList.add(filteredRow);
		}
		
		ClusteringVizPlaySheet drillDownPlaySheet = new ClusteringVizPlaySheet();
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+playSheet.getTitle();
		QuestionPlaySheetStore.getInstance().put(insightID,  drillDownPlaySheet);
		drillDownPlaySheet.setQuery(playSheet.getFullQuery());
		drillDownPlaySheet.setRDFEngine(playSheet.engine);
		drillDownPlaySheet.setQuestionID(insightID);
		drillDownPlaySheet.setTitle(playSheet.getTitle());
		drillDownPlaySheet.drillDownData(masterNames, filteredNames, newList, filteredList);
		drillDownPlaySheet.setSelectedParams(paramCheckboxes);
		
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

	public void setMasterData(String[] masterNames, ArrayList<Object[]> masterList) {
		this.masterNames = masterNames;
		this.masterList = masterList;
	}
	
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
