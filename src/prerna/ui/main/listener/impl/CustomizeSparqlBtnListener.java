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

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

/**
 * Controls the customize SPARQL query button.
 */
public class CustomizeSparqlBtnListener implements IChakraListener {

	JTextArea view = null;

	/**
	 * Method setModel.  Sets the model that the listener will access.
	 * @param model JComponent
	 */
	public void setModel(JComponent model) {
	}

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JToggleButton btnCustomSparql = (JToggleButton) DIHelper.getInstance()
				.getLocalProp(Constants.CUSTOMIZE_SPARQL);
		JTextArea area = (JTextArea) DIHelper.getInstance().getLocalProp(
				Constants.SPARQL_AREA_FIELD);
		JComboBox playSheetComboBox = (JComboBox) DIHelper.getInstance()
				.getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
		JButton btnShowHint = (JButton) DIHelper.getInstance().getLocalProp(
				Constants.SHOW_HINT);
		JButton btnGetQuestionSparql = (JButton) DIHelper.getInstance()
				.getLocalProp(Constants.GET_CURRENT_SPARQL);
		String selectedPlaySheet = (String) playSheetComboBox.getSelectedItem();
		
		// activating custom sparql will enable the sparql text area, show hint btn
		// playsheet menu, and get current question sparql btn
		if (btnCustomSparql.isSelected()) {
			area.setEnabled(true);
			btnShowHint.setEnabled(true);
			playSheetComboBox.setEnabled(true);
			btnGetQuestionSparql.setEnabled(true);
		}
		// deactivating custom sparql will disable the sparql text area, show hint btn
		// playsheet menu, and get current question sparql btn
		if (!btnCustomSparql.isSelected()) {
			area.setEnabled(false);
			btnShowHint.setEnabled(false);
			playSheetComboBox.setEnabled(false);
			btnGetQuestionSparql.setEnabled(false);
		}
		// playSheetComboBox is enabled whenever custom sparql btn is enabled
		if (playSheetComboBox.isEnabled()) {
			// playsheet starting with "*" are those that are not included in predefined
			// list in util.PlaySheetEnum
			if (selectedPlaySheet.startsWith("*")) {
				// set sparql text area to italics and gray
				area.setText("Hint: not available");
				area.setFont(new Font("Tahoma", Font.ITALIC, 11));
				area.setForeground(Color.GRAY);
			} 
			// if sparql area is empty and user switches to a different playsheet
			else if (area.getText().isEmpty()) {
				// set text with current playsheet hint and set font to italics and gray
				area.setText(PlaySheetEnum.getHintFromName(selectedPlaySheet));
				area.setFont(new Font("Tahoma", Font.ITALIC, 11));
				area.setForeground(Color.GRAY);
			} 
			// if sparql area currently has a hint
			else if (area.getText().contains("Hint:")) {
				// set text with current playsheet hint and set font to italics and gray
				area.setText(PlaySheetEnum.getHintFromName(selectedPlaySheet));
				area.setFont(new Font("Tahoma", Font.ITALIC, 11));
				area.setForeground(Color.GRAY);
			}
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextArea) view;
	}
}
