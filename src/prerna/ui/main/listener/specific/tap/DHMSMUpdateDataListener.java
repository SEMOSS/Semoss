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
