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
 * Shows a hint for the custom SPARQL query upon click of the show hint button.
 */
public class ShowSparqlHintListener implements IChakraListener {

	JTextArea view = null;

	/**
	 * Method setModel.  Sets the model that the listener will access.
	 * @param model JComponent
	 */
	public void setModel(JComponent model)
	{
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JTextArea area = (JTextArea)DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
		JButton btnShowHint = (JButton)DIHelper.getInstance().getLocalProp(Constants.SHOW_HINT);
		JComboBox playSheetComboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.PLAYSHEET_COMBOBOXLIST);
		JToggleButton btnCustomSparql = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.CUSTOMIZE_SPARQL);
		String selectedPlaySheet = (String)playSheetComboBox.getSelectedItem();
		
		// only allow use of show hint btn when custom sparql btn is selected
		if(btnCustomSparql.isSelected()){
			if(btnShowHint.isEnabled()){
				// playsheet starting with "*" are those that are not included in predefined
				// list in util.PlaySheetEnum
				if(selectedPlaySheet.startsWith("*"))
				{
					area.setText("Hint: not available");
					area.setFont(new Font("Tahoma", Font.ITALIC, 11));
					area.setForeground(Color.GRAY);
				}
				else{
					// display hint in sparql area
					area.setText(PlaySheetEnum.getHintFromName(selectedPlaySheet));
					area.setFont(new Font("Tahoma", Font.ITALIC, 11));
					area.setForeground(Color.GRAY);
				}
			}
		}
	
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextArea)view;	
	}
}
