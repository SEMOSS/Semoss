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

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import prerna.rdf.main.ImportRDBMSProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RDBMSTestConnectionCreateSchemaBtnListener extends AbstractListener {
	
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> dbType = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX);
		JTextField usernameField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_USERNAME_FIELD);
		JPasswordField passwordField = (JPasswordField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_PW_FIELD);
		JTextField dbImportURLField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);
		JButton btnGetRDBMSSchema = (JButton)DIHelper.getInstance().getLocalProp(Constants.GET_RDBMS_SCHEMA);
		JButton btnTestRDBMSConnection = (JButton)DIHelper.getInstance().getLocalProp(Constants.TEST_RDBMS_CONNECTION);
		
		JButton btn = (JButton)e.getSource();
		btn.setEnabled(false);
		boolean testConn = false;
		if(btn.getName().equals(btnTestRDBMSConnection.getName()))
		{
			btn.setText("Connecting...");
			testConn = true;
		}
		else
		{
			btn.setText("Creating Excel...");
		}
		
		String username = usernameField.getText();
		char[] password = passwordField.getPassword();
		String url = dbImportURLField.getText();
		ImportRDBMSProcessor t = new ImportRDBMSProcessor();
		
		boolean validConnection = false;
		
		if(username != null &&  password != null && url != null && !username.isEmpty() && password.length > 0 && !url.isEmpty())
		{	
			validConnection = t.checkConnection(dbType.getSelectedItem().toString(), url, username, password);
			
			if(validConnection && testConn) {
				Utility.showMessage("Valid connection!");
			} 
			else if(validConnection && !testConn)
			{
				boolean excelCreated = t.processRDBMSSchema(dbType.getSelectedItem().toString(), url, username, password);
				if(excelCreated)
				{
					Utility.showMessage("Excel file located in Folder: \n" + DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\rdbms");
				}
				else
				{
					Utility.showError("Error creating Loading Sheet");
				}
			}
			else 
			{
				Utility.showError("Could not connect to " + username + "@" + url);
			}
			
			//zero out the password after use
			for(int i = 0; i < password.length; i++)
			{
				password[i] = 0;
			}
		}
		else
		{
			Utility.showError("Could not connect to " + username + "@" + url + "\n Please make sure username, password, and url fields are filled.");
		}

		
		btn.setEnabled(true);
		if(btn.getName().equals(btnTestRDBMSConnection.getName()))
		{
			btn.setText("Test Connection");
		}
		else
		{
			btn.setText("Get RDBMS Schema");
		}
		
	}

	@Override
	public void setView(JComponent view) {
		
	}

}