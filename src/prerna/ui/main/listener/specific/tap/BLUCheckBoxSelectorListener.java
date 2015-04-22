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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.JCheckBox;

import prerna.ui.components.specific.tap.SysOptCheckboxListUpdater;
import prerna.ui.swing.custom.SelectScrollList;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class BLUCheckBoxSelectorListener implements ActionListener {

	private SysOptCheckboxListUpdater checkboxListUpdater;
	private SelectScrollList scrollList;
	
	private JCheckBox allElemCheckBox, hsdCheck, hssCheck, fhpCheck, dhmsmCheck;

	public BLUCheckBoxSelectorListener (SysOptCheckboxListUpdater checkboxListUpdater,SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox hsdCheck,JCheckBox hssCheck,JCheckBox fhpCheck,JCheckBox dhmsmCheck) {
		this.checkboxListUpdater = checkboxListUpdater;
		this.scrollList = scrollList;
		this.allElemCheckBox = allElemCheckBox;
		this.dhmsmCheck = dhmsmCheck;
		this.hsdCheck = hsdCheck;
		this.hssCheck = hssCheck;
		this.fhpCheck = fhpCheck;
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JCheckBox)e.getSource()).getName().equals(allElemCheckBox.getName()))
		{
			if(allElemCheckBox.isSelected()) {
				unselectAllCheckBoxes();
				scrollList.selectAll();
			} else {
				unselectAllCheckBoxes();
				scrollList.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		
		Vector<String> dataToSelect = checkboxListUpdater.getSelectedBLUList(dhmsmCheck.isSelected(), hsdCheck.isSelected(), hssCheck.isSelected(), fhpCheck.isSelected());
		scrollList.setSelectedValues(dataToSelect);
	}
	
	private void unselectAllCheckBoxes() {
		hsdCheck.setSelected(false);
		hssCheck.setSelected(false);
		fhpCheck.setSelected(false);
		dhmsmCheck.setSelected(false);
	}

}
