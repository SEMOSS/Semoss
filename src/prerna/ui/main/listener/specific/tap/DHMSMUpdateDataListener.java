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
import java.util.ArrayList;
import java.util.Enumeration;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SelectRadioButtonPanel;
import prerna.ui.components.specific.tap.SourceSelectPanel;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * Listener for sourceReportGenButton
 */
public class DHMSMUpdateDataListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(DHMSMUpdateDataListener.class.getName());
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		
		//get capability list and then update the list of data objects shown
		ArrayList<String> capabilitiesSelected = new ArrayList<String>();
		SourceSelectPanel dhmsmCapSelPanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.DHMSM_CAPABILITY_SELECT_PANEL);
		Enumeration<String> enumKey = dhmsmCapSelPanel.checkBoxHash.keys();
		while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JCheckBox checkBox = (JCheckBox) dhmsmCapSelPanel.checkBoxHash.get(key);
				if (checkBox.isSelected())
					capabilitiesSelected.add(key);
		}

		
		SelectRadioButtonPanel radioSelPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
		radioSelPanel.getDataObjectsFromCapabilities(capabilitiesSelected);
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
