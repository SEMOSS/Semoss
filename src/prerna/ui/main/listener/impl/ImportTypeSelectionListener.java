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

import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the type of import.  Tied to the JComboBox in the Import Data section of the DB modification pane.
 */
public class ImportTypeSelectionListener extends AbstractListener {

	public JComponent view = null;
	
	// will have 2 string arrays one for the perspective and one for the question
	Hashtable model = null;
	static final Logger logger = LogManager.getLogger(ImportTypeSelectionListener.class.getName());
	
	// needs to find what is being selected from event
	// based on that refresh the view of questions for that given perspective
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox bx = (JComboBox)e.getSource();
		JComboBox typeBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.IMPORT_TYPE_COMBOBOX);
		JLabel typeLbl = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_TYPE_LABEL);
		JPanel importPanel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_PANEL);
		JLabel lblDBName = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_ENTERDB_LABEL);
		JLabel lblFileImport = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_LABEL);
		JLabel lblDBImportURL = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_LABEL);
		JLabel lblDBImportDriverType = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_LABEL);
		JLabel lblDBImportUsername = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_LABEL);
		JLabel lblDBImportPW = (JLabel)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_LABEL);
		JTextField dbImportURLField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);
		JComboBox dbImportRDBMSDriverComboBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX);
		JTextField dbImportUsernameField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD);
		JTextField dbImportPWField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD);
		JTextField dbNameField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.DB_NAME_FIELD);
		JTextField fileImportField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_FILE_FIELD);
		JButton btnFileBrowse = (JButton)DIHelper.getInstance().getLocalProp(Constants.IMPORT_BUTTON_BROWSE);
		JButton btnImport = (JButton)DIHelper.getInstance().getLocalProp(Constants.IMPORT_BUTTON);
		JButton btnTestRDBMSConnection = (JButton)DIHelper.getInstance().getLocalProp(Constants.TEST_RDBMS_CONNECTION);
		JButton btnGetRDBMSSchema = (JButton)DIHelper.getInstance().getLocalProp(Constants.GET_RDBMS_SCHEMA);
		JTextField mapText = (JTextField)DIHelper.getInstance().getLocalProp(Constants.MAP_TEXT_FIELD);
		JButton advancedButton = (JButton)DIHelper.getInstance().getLocalProp(Constants.ADVANCED_IMPORT_OPTIONS_BUTTON);
		JPanel advancedPanel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.ADVANCED_IMPORT_OPTIONS_PANEL);
		String selection = bx.getSelectedItem() + "";
		if(selection.equals("Add to existing database engine") )
		{
			typeBox.setVisible(true);
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel format", "CSV format"}));
			typeLbl.setVisible(true);
			importPanel.setVisible(true);
			lblDBName.setVisible(false);
			lblFileImport.setVisible(true);
			dbNameField.setVisible(false);
			fileImportField.setVisible(true);
			mapText.setVisible(false);
			advancedButton.setVisible(false);
			advancedPanel.setVisible(false);
			lblDBImportURL.setVisible(false);
			lblDBImportDriverType.setVisible(false);
			lblDBImportUsername.setVisible(false);
			lblDBImportPW.setVisible(false);
			dbImportURLField.setVisible(false);
			dbImportRDBMSDriverComboBox.setVisible(false);
			dbImportUsernameField.setVisible(false);
			dbImportPWField.setVisible(false);
			btnTestRDBMSConnection.setVisible(false);
			btnGetRDBMSSchema.setVisible(false);
		}
		else if(selection.equals("Modify/Replace data in existing engine"))
		{
			typeBox.setVisible(true);
			typeBox.removeAll();
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel format"}));
			typeBox.setSelectedIndex(0);
			typeLbl.setVisible(true);
			importPanel.setVisible(true);
			lblDBName.setVisible(false);
			lblFileImport.setVisible(true);
			dbNameField.setVisible(false);
			fileImportField.setVisible(true);
			mapText.setVisible(false);
			advancedButton.setVisible(false);
			advancedPanel.setVisible(false);
			lblDBImportURL.setVisible(false);
			lblDBImportDriverType.setVisible(false);
			lblDBImportUsername.setVisible(false);
			lblDBImportPW.setVisible(false);
			dbImportURLField.setVisible(false);
			dbImportRDBMSDriverComboBox.setVisible(false);
			dbImportUsernameField.setVisible(false);
			dbImportPWField.setVisible(false);
			btnTestRDBMSConnection.setVisible(false);			
			btnGetRDBMSSchema.setVisible(false);
		}
		else if (selection.equals("Create new database engine"))
		{
			typeBox.setVisible(true);
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel format", "CSV format", "Natural Language Processing", "Optical Character Recognition + NLP"}));
			typeLbl.setVisible(true);
			importPanel.setVisible(true);
			lblDBName.setVisible(true);
			lblFileImport.setVisible(true);
			dbNameField.setVisible(true);
			fileImportField.setVisible(true);
			mapText.setVisible(true);
			advancedButton.setVisible(true);
			if(!advancedPanel.isVisible() && advancedButton.getText().contains("Hide")) 
				advancedButton.setText(advancedButton.getText().replace("Hide", "Show"));
			lblDBImportURL.setVisible(false);
			lblDBImportDriverType.setVisible(false);
			lblDBImportUsername.setVisible(false);
			lblDBImportPW.setVisible(false);
			dbImportURLField.setVisible(false);
			dbImportRDBMSDriverComboBox.setVisible(false);
			dbImportUsernameField.setVisible(false);
			dbImportPWField.setVisible(false);
			btnTestRDBMSConnection.setVisible(false);
			btnGetRDBMSSchema.setVisible(false);
		}
		else if (selection.equals("Create new RDBMS connection")) {
			typeBox.setVisible(true);
			typeBox.removeAll();
			typeBox.setModel(new DefaultComboBoxModel(new String[] {"Microsoft Excel format"}));
			typeLbl.setVisible(true);
			importPanel.setVisible(true);
			lblDBName.setVisible(true);
			lblFileImport.setVisible(true);
			dbNameField.setVisible(true);
			fileImportField.setVisible(true);
			mapText.setVisible(true);
			advancedButton.setVisible(false);
			advancedPanel.setVisible(false);
			lblDBImportURL.setVisible(true);
			lblDBImportDriverType.setVisible(true);
			lblDBImportUsername.setVisible(true);
			lblDBImportPW.setVisible(true);
			dbImportURLField.setVisible(true);
			dbImportRDBMSDriverComboBox.setVisible(true);
			dbImportUsernameField.setVisible(true);
			dbImportPWField.setVisible(true);
			btnTestRDBMSConnection.setVisible(true);
			btnGetRDBMSSchema.setVisible(true);
		}
		else if (selection.equals("Select a database import method"))
		{
			importPanel.setVisible(false);
			typeBox.setVisible(false);
			typeLbl.setVisible(false);
		}

	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		logger.debug("View is set " + view);
		this.view = view;
		
	}

}
