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

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Determine which user options can be selected if user selects a system specific or generic transition cost report type
 * The user selects which report type in TransReportTypecomboBox
 */

public class TransReportTypeComboBoxListener  extends AbstractListener {

	static final Logger logger = LogManager.getLogger(TransReportTypeComboBoxListener.class.getName());
	
	// needs to find what is being selected from event
	// based on that either hide or show additional criteria
	
	/**
	 * Sets specific user options visible based on the type of transition cost report selected in TransReportTypecomboBox
	 * The two options in the combobox are generic or system specific
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox bx = (JComboBox)e.getSource();
		String reportType = bx.getSelectedItem() + "";

		JPanel sysDropdownPanel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_SYSTEM_DROP_DOWN_PANEL);
		JCheckBox dataFedCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_FED);
		JCheckBox dataConsumerCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_CONSUMER);
		JCheckBox bluProviderCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_PROVIDER);
		JCheckBox dataGenericCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_GENERIC);
		JCheckBox bluGenericCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_GENERIC);
		
		if(reportType.contains("Generic")){
			//hide additional components
			sysDropdownPanel.setVisible(false);
			dataFedCheck.setVisible(false);
			dataConsumerCheck.setVisible(false);
			bluProviderCheck.setVisible(false);
			dataGenericCheck.setVisible(true);
			bluGenericCheck.setVisible(true);
		}
		else if (reportType.contains("Specific")){
			//show additional components
			sysDropdownPanel.setVisible(true);
			dataFedCheck.setVisible(true);
			dataConsumerCheck.setVisible(true);
			bluProviderCheck.setVisible(true);
			dataGenericCheck.setVisible(false);
			bluGenericCheck.setVisible(false);
		}
		logger.debug("Report Type " + reportType + " selected");

	}

	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}