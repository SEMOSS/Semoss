/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
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
