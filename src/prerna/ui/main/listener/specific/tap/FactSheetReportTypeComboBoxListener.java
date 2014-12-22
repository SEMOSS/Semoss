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

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Listener for FactSheetReportTypecomboBox on the MHS TAP tab
 * Results in the creation of fact sheets for either all systems or a specified system
 */
public class FactSheetReportTypeComboBoxListener extends AbstractListener {

	static final Logger logger = LogManager.getLogger(FactSheetReportTypeComboBoxListener.class.getName());

	// needs to find which report type is being selected from event
	// based on that either hide or show additional criteria

	/**
	 * This is executed when the FactSheetReportTypecomboBox is pressed by the user
	 * When user wants to output a fact sheet for a specific system, a JComboBox is set visible showing all the different systems to choose from
	 * If generating reports for all systems, JComboBox is not visible
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JComboBox bx = (JComboBox)arg0.getSource();
		String reportType = bx.getSelectedItem() + "";

		JPanel sysDropdownPanel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.FACT_SHEET_SYSTEM_DROP_DOWN_PANEL);

		if(reportType.contains("All Systems")){
			//hide additional components
			sysDropdownPanel.setVisible(false);			
		}
		else if (reportType.contains("Specific")){
			//show additional components
			sysDropdownPanel.setVisible(true);
		}
		logger.debug("Report Type " + reportType + " selected");
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {

	}

}
