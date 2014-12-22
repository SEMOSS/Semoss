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

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.InterfaceReportProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Listener for btnFactSheetReport on the MHS TAP tab
 * Results in the creation of fact sheets for either all systems or a specified system
 */
public class InterfaceReportListener implements IChakraListener {

	InterfaceReportProcessor processor = new InterfaceReportProcessor();
	static final Logger logger = LogManager.getLogger(InterfaceReportListener.class.getName());
	ArrayList queryArray = new ArrayList();

	/**
	 * This is executed when the btnFactSheetReport is pressed by the user
	 * Calls FactSheetProcessor to generate all the information from the queries to write onto the fact sheet
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			processor.generateInterfaceReport();	
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {

	}

}
