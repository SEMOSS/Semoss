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
import java.util.List;

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
	private String[] columnHeaders;
	private ArrayList<JCheckBox> paramCheckboxes;

	/**
	 * Updates the parameters to cluster on based on the params selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		List<String> skipColumns = new ArrayList<String>();
		
		for(int i = 0; i < paramCheckboxes.size(); i++) {
			if(paramCheckboxes.get(i).isSelected()) {
				skipColumns.add(columnHeaders[i+1]);
			}
		}

		playSheet.skipAttributes(skipColumns);
	}

	public void setPlaySheet(ClusteringVizPlaySheet playSheet) {
		this.playSheet = playSheet;
	}
	
	public void setCheckBoxes(ArrayList<JCheckBox> paramCheckboxes) {
		this.paramCheckboxes = paramCheckboxes;
	}

	public void setColumnHeaders(String[] columnHeaders) {
		this.columnHeaders = columnHeaders;
	}
	
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
