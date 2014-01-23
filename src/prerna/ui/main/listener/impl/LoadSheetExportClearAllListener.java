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

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.helpers.EntityFillerForSubClass;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Populates the subject/predicate/object combo boxes during an export.
 */
public class LoadSheetExportClearAllListener implements IChakraListener {

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		for(int i = 1; i <= Constants.MAX_EXPORTS; i++) {
			ParamComboBox subject = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + i);
			ParamComboBox object = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + i);
			ParamComboBox relationship = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + i);
			
			if(i != 1) {
				DefaultComboBoxModel model = new DefaultComboBoxModel(new String[0]);
				subject.setModel(model);
				subject.setEditable(false);
				object.setModel(model);
				object.setEditable(false);
				relationship.setModel(model);
				relationship.setEditable(false);
				
				subject.setVisible(false);
				object.setVisible(false);
				relationship.setVisible(false);
			}
			
			ParamComboBox exportDataSourceComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
			populateLoadSheetExportComboBoxes(exportDataSourceComboBox.getSelectedItem().toString());
			
			JButton addExportButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_ADD_EXPORT_BUTTON);
			JLabel maxLimitLabel = (JLabel)DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_MAX_LIMIT_MESSAGE);
			addExportButton.setVisible(true);
			maxLimitLabel.setVisible(false);
		}
	}
	
	/**
	 * Method populateLoadSheetExportComboBoxes.
	 * @param repo String The repository.
	 */
	public void populateLoadSheetExportComboBoxes(String repo) {
		ArrayList<JComboBox> boxes = new ArrayList<JComboBox>();
		for(int i = 1; i <= Constants.MAX_EXPORTS; i++) {
			ParamComboBox subjectNodeTypeComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + i);
			boxes.add(subjectNodeTypeComboBox);
		}
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
		EntityFillerForSubClass entityFillerSC = new EntityFillerForSubClass();
		entityFillerSC.boxes = boxes;
		entityFillerSC.engine = engine;
		entityFillerSC.parent = "Concept";
		Thread aThread = new Thread(entityFillerSC);
		aThread.run();
		
		DefaultComboBoxModel model = new DefaultComboBoxModel(new String[0]);
		ParamComboBox objectNodeTypeComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "1");
		objectNodeTypeComboBox.setModel(model);
		objectNodeTypeComboBox.setEditable(false);
		ParamComboBox nodeRelationshipComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "1");
		nodeRelationshipComboBox.setModel(model);
		nodeRelationshipComboBox.setEditable(false);
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}

}
