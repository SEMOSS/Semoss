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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.FileReaderException;
import prerna.poi.specific.DHMSMDataAccessLatencyFileImporter;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SelectRadioButtonPanel;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;


/**
 * Listener for sourceReportGenButton
 */
public class DHMSMUpdateDataFromFileListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(DHMSMUpdateDataFromFileListener.class.getName());
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		//get data objects that are checked and put them in the data object hash
		Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();
		Hashtable<String,String> dataLatencyTypeHash = new Hashtable<String,String>();

		JTextField dataAccessLatencyFileField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.SELECT_DATA_ACCESS_FILE_JFIELD);
		if(dataAccessLatencyFileField.getText()!=null&&dataAccessLatencyFileField.getText().length()>0)
		{
			DHMSMDataAccessLatencyFileImporter dataAccessImporter = new DHMSMDataAccessLatencyFileImporter();
			try {
				dataAccessImporter.importFile(dataAccessLatencyFileField.getText());
				dataAccessTypeHash = dataAccessImporter.getDataAccessTypeHash();
				dataLatencyTypeHash = dataAccessImporter.getDataLatencyTypeHash();
			} catch (RuntimeException e) {
				Utility.showError("<html>Error with Selected File.</html>");
				return;
			} catch (FileReaderException e) {
				Utility.showError("<html>Error with Selected File.</html>");
				return;
			}
			

		}
		else
		{
			Utility.showError("<html>Please select a file.</html>");
			return;
		}
		if(dataLatencyTypeHash.isEmpty()&&dataAccessTypeHash.isEmpty())
		{
			Utility.showError("<html>Please select at least one data object.</html>");
			return;
		}
		SelectRadioButtonPanel radioSelPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
		radioSelPanel.getDataObjectsFromHashes(dataAccessTypeHash,dataLatencyTypeHash);

	
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
