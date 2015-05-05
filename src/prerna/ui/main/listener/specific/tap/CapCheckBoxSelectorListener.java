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
import prerna.engine.api.IEngine;
import prerna.ui.swing.custom.SelectScrollList;
/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class CapCheckBoxSelectorListener implements ActionListener {

	private SysOptCheckboxListUpdater checkboxListUpdater;
	private SelectScrollList scrollList;
	
	private JCheckBox allElemCheckBox, dhmsmCapCheckBox, hsdCapCheckBox, hssCapCheckBox, fhpCapCheckBox;
	
	public CapCheckBoxSelectorListener(SysOptCheckboxListUpdater checkboxListUpdater, SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox dhmsmCapCheckBox,JCheckBox hsdCapCheckBox,JCheckBox hssCapCheckBox,JCheckBox fhpCapCheckBox) {

		this.checkboxListUpdater = checkboxListUpdater;
		this.scrollList = scrollList;
		this.allElemCheckBox = allElemCheckBox;
		this.dhmsmCapCheckBox = dhmsmCapCheckBox;
		this.hsdCapCheckBox = hsdCapCheckBox;
		this.hssCapCheckBox = hssCapCheckBox;
		this.fhpCapCheckBox = fhpCapCheckBox;
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
		
		Vector<String> capabilitiessToSelect = checkboxListUpdater.getSelectedCapabilityList(dhmsmCapCheckBox.isSelected(), hsdCapCheckBox.isSelected(), hssCapCheckBox.isSelected(), fhpCapCheckBox.isSelected());
		scrollList.setSelectedValues(capabilitiessToSelect);
	}

	private void unselectAllCheckBoxes() {
		dhmsmCapCheckBox.setSelected(false);
		hsdCapCheckBox.setSelected(false);
		hssCapCheckBox.setSelected(false);
		fhpCapCheckBox.setSelected(false);
	}

}
