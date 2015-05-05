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
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.JCheckBox;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Selects or unselects all the checkboxes in a given list
 */
public class SelectCheckboxesListener implements ActionListener {
	private static final Logger LOGGER = LogManager.getLogger(SelectCheckboxesListener.class.getName());
	
	private ArrayList<JCheckBox> checkboxes;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JCheckBox selectAllCheckBox = (JCheckBox)e.getSource();
		Boolean isSelected = selectAllCheckBox.isSelected();
		if(checkboxes==null) {
			LOGGER.error("Cannot select all checkboxes because arraylist is undefined");
			return;
		}
		for(JCheckBox checkbox : checkboxes) {
			if(checkbox.isEnabled())
				checkbox.setSelected(isSelected);
		}
	}

	public void setCheckboxes(ArrayList<JCheckBox> checkboxes) {
		this.checkboxes = checkboxes;
	}

}
