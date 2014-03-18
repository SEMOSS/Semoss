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
import java.io.File;

import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.ui.components.api.IChakraListener;

/**
 * Attached to the Browse File button in playpane, opens a file browser window to view files.
 */
public class DHMSMDataAccessFileBrowseListener implements IChakraListener {

	JTextField view = null;

	Logger log = Logger.getLogger(getClass());

	// TODO Unused, Delete
	public void setModel(JComponent model) {
	}

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// I just need to show the file chooser and set the action performed to
		// a file chooser class
		JFileChooser jfc = new JFileChooser();

		jfc.setMultiSelectionEnabled(true);
		jfc.setCurrentDirectory(new java.io.File("."));
		int retVal = jfc.showOpenDialog((JComponent) e.getSource());
		// Handle open button action.
		if (retVal == JFileChooser.APPROVE_OPTION) {
			File[] files = jfc.getSelectedFiles();
			// This is where a real application would open the file.
			String fileNames = "";
			String filePaths = "";
			for (File f : files) {
				fileNames = fileNames + f.getName() + ";";
				filePaths = filePaths + f.getAbsolutePath() + ";";
			}
			log.info("Opening: " + fileNames + ".");
			view.setText(filePaths);
		} else {
			log.info("Open command cancelled by user.");
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or
	 * modify when an action event occurs.
	 * 
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.view = (JTextField) view;

	}

}
