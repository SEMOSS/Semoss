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

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;

import org.apache.log4j.Logger;

import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Listener for FactSheetReportTypecomboBox on the MHS TAP tab
 * Results in the creation of fact sheets for either all systems or a specified system
 */
public class FactSheetReportTypeComboBoxListener extends AbstractListener {

	Logger logger = Logger.getLogger(getClass());

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
