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

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;

import org.apache.log4j.Logger;

import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Determine which user options can be selected if user selects a system specific or generic transition cost report type
 * The user selects which report type in TransReportTypecomboBox
 */

public class TransReportTypeComboBoxListener  extends AbstractListener {

	Logger logger = Logger.getLogger(getClass());
	
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