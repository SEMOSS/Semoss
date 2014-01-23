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
import javax.swing.JComponent;
import org.apache.log4j.Logger;

import prerna.poi.specific.FactSheetImageExportProcessor;
import prerna.ui.components.api.IChakraListener;

/**
 * Listener for btnFactSheetImageExport on the MHS TAP tab
 * Results in the creation of fact sheets for either all systems or a specified system
 */
public class FactsReportImageExportButtonListener implements IChakraListener {

	Logger logger = Logger.getLogger(getClass());
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
