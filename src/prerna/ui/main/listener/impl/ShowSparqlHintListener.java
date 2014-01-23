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
