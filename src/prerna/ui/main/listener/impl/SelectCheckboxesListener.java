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
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

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
			else //if the checkbox is disabled, then needs to be selected
				checkbox.setSelected(true);
		}
	}

	public void setCheckboxes(ArrayList<JCheckBox> checkboxes) {
		this.checkboxes = checkboxes;
	}

}
