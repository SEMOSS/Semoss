/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.poi.specific.FactSheetProcessor;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DHMSMDispositionFactSheetProcessor;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnFactSheetReport on the MHS TAP tab Results in the creation of
 * fact sheets for either all systems or a specified system
 */
public class FactSheetListener implements IChakraListener {

	FactSheetProcessor processor;
	static final Logger logger = LogManager.getLogger(FactSheetListener.class.getName());
	ArrayList queryArray = new ArrayList();

	/**
	 * This is executed when the btnFactSheetReport is pressed by the user Calls
	 * FactSheetProcessor to generate all the information from the queries to
	 * write onto the fact sheet
	 * 
	 * @param arg0
	 *            ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JComboBox reportTypeToggleComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(
				ConstantsTAP.FACT_SHEET_REPORT_TYPE_TOGGLE_COMBO_BOX);
		JComboBox reportSystemToggleComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(
				ConstantsTAP.FACT_SHEET_REPORT_SYSTEM_TOGGLE_COMBO_BOX);
		String reportType = (String) reportTypeToggleComboBox.getSelectedItem();
		String systemSelectionType = (String) reportSystemToggleComboBox.getSelectedItem();
		String system = null;
		
		DHMSMHelper dhelp = new DHMSMHelper();
		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(
				"TAP_Core_Data");
		dhelp.runData(engine);
		
		if (reportType.contains("Services")) {
			processor = new FactSheetProcessor();
		} else if (reportType.contains("MHS GENESIS Disposition")) {
			try {
				processor = new DHMSMDispositionFactSheetProcessor();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
				Utility.showError(e.getMessage());
			}
		}
		
		if (systemSelectionType.contains("Specific")) {
			ParamComboBox systemComboBox = (ParamComboBox) DIHelper
					.getInstance().getLocalProp(
							ConstantsTAP.FACT_SHEET_SYSTEM_SELECT_COMBO_BOX);
			system = (String) systemComboBox.getSelectedItem();
			processor.setDHMSMHelper(dhelp);
			processor.generateSystemReport(system, true);
		} else if (systemSelectionType.contains("All Systems")) {
			processor.setDHMSMHelper(dhelp);
			processor.generateReports();
		}
	}

	/**
	 * Override method from IChakraListener
	 * 
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {

	}

}
