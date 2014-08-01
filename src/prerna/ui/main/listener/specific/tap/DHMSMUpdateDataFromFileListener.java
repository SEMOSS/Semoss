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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

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

	Logger logger = Logger.getLogger(getClass());
	
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
			} catch (Exception e) {
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
