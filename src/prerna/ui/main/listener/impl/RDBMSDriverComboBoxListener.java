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

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDBMSDriverComboBoxListener extends AbstractListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		
		JComboBox<String> rdbmsDriver = (JComboBox<String>)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_DRIVER_COMBOBOX);
		JTextField rdbmsUrlField = (JTextField)DIHelper.getInstance().getLocalProp(Constants.IMPORT_RDBMS_URL_FIELD);
		
		String driverType = (String) rdbmsDriver.getSelectedItem();
		
		if(driverType.equals("MySQL"))
		{
			rdbmsUrlField.setText("jdbc:mysql://<hostname>[:port]/<DBname>");
		}
		else if(driverType.equals("Oracle"))
		{
			rdbmsUrlField.setText("jdbc:oracle:thin:@<hostname>[:port]/<service or sid>[-schema name]");
		}
		else if(driverType.equals("MS SQL Server"))
		{
			rdbmsUrlField.setText("jdbc:sqlserver://<hostname>[:port];databaseName=<DBname>");
		}
		else if(driverType.equals("Aster Database")) {
			rdbmsUrlField.setText("jdbc:ncluster://192.168.100.100/beehive");
		}
		
		rdbmsDriver.removeItem("Select Relational Database Type");
	}
	

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}
	
}
