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
import java.util.ArrayList;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.ParamComboBox;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DataInterfaceWithDHMSMProcessor;
import prerna.ui.components.specific.tap.SystemInfoGenProcessor;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Listener for btnDataInterfaceWithDHMSM on the MHS TAP tab
 * Results in the creation of a report of all the data interfacing with DHMSM
 */
public class DataInterfaceWithDHMSMButtonListener implements IChakraListener {

	DataInterfaceWithDHMSMProcessor processor = new DataInterfaceWithDHMSMProcessor();
	static final Logger logger = LogManager.getLogger(DataInterfaceWithDHMSMButtonListener.class.getName());

	/**
	 * This is executed when the btnFactSheetReport is pressed by the user
	 * Calls FactSheetProcessor to generate all the information from the queries to write onto the fact sheet
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		processor.generateDataInterfaceReport();

	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {

	}

}
