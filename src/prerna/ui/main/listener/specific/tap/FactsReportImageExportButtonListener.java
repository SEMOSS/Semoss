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

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.FactSheetImageExportProcessor;
import prerna.ui.components.api.IChakraListener;

/**
 * Listener for btnFactSheetImageExport on the MHS TAP tab
 * Results in the creation of fact sheets for either all systems or a specified system
 */
public class FactsReportImageExportButtonListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(FactsReportImageExportButtonListener.class.getName());
	FactSheetImageExportProcessor processor = new FactSheetImageExportProcessor();

	/**
	 * This is executed when the btnFactSheetImageExport is pressed by the user
	 * Calls CONUSMapExporter and HealthGridExporterprocess the query results
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		processor.runImageExport();
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}
}
