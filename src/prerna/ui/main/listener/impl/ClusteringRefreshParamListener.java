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

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import prerna.ui.components.playsheets.ClusteringVizPlaySheet;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class ClusteringRefreshParamListener extends AbstractListener {
	
	//need to pass the playsheet, the checkboxes, the names, and the master data list,
	//sends back the updates names and data list
	private ClusteringVizPlaySheet playSheet;
	private ArrayList<JCheckBox> paramCheckboxes;
	private String[] masterNames;
	private ArrayList<Object []> masterList;
	
	/**
	 * Updates the parameters to cluster on based on the params selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		Integer numberSelected = 0;
		for(int i=0;i<paramCheckboxes.size();i++) {
			if(paramCheckboxes.get(i).isSelected())
				numberSelected++;
		}
		
		String[] filteredNames = new String[numberSelected+1];
		filteredNames[0] = masterNames[0];
		int colInd=1;
		for(int i=0;i<paramCheckboxes.size();i++) {
			if(paramCheckboxes.get(i).isSelected()) {
				filteredNames[colInd] = masterNames[1+i];
				colInd++;
			}
		}

		ArrayList<Object[]> filteredList = new ArrayList<Object []>();
		for(Object[] row : masterList) {
			Object[] filteredRow = new Object[numberSelected+1];
			filteredRow[0] = row[0];//whatever object name we're clustering on
			colInd=1;
			for(int i=0;i<paramCheckboxes.size();i++) {
				if(paramCheckboxes.get(i).isSelected()) {
					filteredRow[colInd] = row[1+i];
					colInd++;
				}
			}
			filteredList.add(filteredRow);
		}
		
		playSheet.filterData(filteredNames,filteredList);

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
