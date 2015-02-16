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
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DHMSMSystemSelectPanel;
import prerna.ui.components.specific.tap.ServiceICDCostAnalyzer;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Listener for serviceSelectionBtn in MHS Tab
 * Used to create a new panel showing all the services for a selected database
 */
public class ICDServiceCostBtnListener implements IChakraListener {

	JTextArea view = null;
	DHMSMSystemSelectPanel sysPanel;
	/**
	 * Sets visible a new frame when user presses serviceSelectionBtn
	 * Allows the user to select which services to include when producing Transition Cost Reports
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		ArrayList<String> systems = sysPanel.getSelectedSystems();
		JTextField susField = (JTextField)DIHelper.getInstance().getLocalProp(ConstantsTAP.SER_ICD_SUS_FIELD);
		double susPer = Double.parseDouble(susField.getText());
		JTextField icdCostField = (JTextField)DIHelper.getInstance().getLocalProp(ConstantsTAP.SER_ICD_COST_FIELD);
		double icdCost = Double.parseDouble(icdCostField.getText());
		JTextField rateField = (JTextField)DIHelper.getInstance().getLocalProp(ConstantsTAP.SER_ICD_RATE_FIELD);
		double hourlyRate = Double.parseDouble(rateField.getText());
		
		
		ServiceICDCostAnalyzer serCost = new ServiceICDCostAnalyzer(systems);
		serCost.setConstants(hourlyRate, icdCost, susPer);
		serCost.runServiceResults();

	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextArea)view;
	}

	public void setSystemSelectPanel(DHMSMSystemSelectPanel sysPanel)
	{
		this.sysPanel = sysPanel;
	}
}
