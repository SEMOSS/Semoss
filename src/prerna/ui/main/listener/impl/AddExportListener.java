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

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * TODO Replaced by node editor?
 * Adds a node or a relationship to the graph selected.
 */
public class AddExportListener implements IChakraListener {

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		String genericSubjectNodeTypeComboBox = "subjectNodeTypeComboBox";
		String genericObjectNodeTypeComboBox = "objectNodeTypeComboBox";
		String genericNodeRelationshipComboBox = "nodeRelationshipComboBox";
		
		for(int i = 2; i <= Constants.MAX_EXPORTS; i++) {
			JComboBox subject = (JComboBox)DIHelper.getInstance().getLocalProp(genericSubjectNodeTypeComboBox + i);
			JComboBox object = (JComboBox)DIHelper.getInstance().getLocalProp(genericObjectNodeTypeComboBox + i);
			JComboBox relationship = (JComboBox)DIHelper.getInstance().getLocalProp(genericNodeRelationshipComboBox + i);
			
			if(subject.isVisible() == false) {
				subject.setVisible(true);
				object.setVisible(true);
				relationship.setVisible(true);
				
				if(i == Constants.MAX_EXPORTS) {
					JButton button = (JButton) arg0.getSource();
					JLabel maxLimitLabel = (JLabel)DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_MAX_LIMIT_MESSAGE);
					button.setVisible(false);
					maxLimitLabel.setVisible(true);
				}
				
				break;
			}
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}

}
