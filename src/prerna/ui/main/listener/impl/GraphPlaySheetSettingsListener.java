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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import org.apache.log4j.Logger;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Gets all settings check boxes relating to the graph play sheet,
 *  grabs current state of all settings, and updates state of all settings according to check box values.
 */
public class GraphPlaySheetSettingsListener implements IChakraListener{

	Logger logger = Logger.getLogger(getClass());
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		logger.info("Registering check box");
		JCheckBox sudowlCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.sudowlCheck);
		JCheckBox propCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.propertyCheck);
		JCheckBox searchCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.searchCheck);
		JCheckBox highQualityExportCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.highQualityExportCheck);
		
		boolean sudowl = sudowlCheck.isSelected();
		logger.info("GPSSudowl: " + sudowl);
		DIHelper.getInstance().getCoreProp().put(Constants.GPSSudowl, sudowl);
		
		boolean prop = propCheck.isSelected();
		logger.info("GPSProp: " + prop);
		DIHelper.getInstance().getCoreProp().put(Constants.GPSProp, prop);
		
		boolean search = searchCheck.isSelected();
		logger.info("GPSSearch: " + search);
		DIHelper.getInstance().getCoreProp().put(Constants.GPSSearch, search);
		
		boolean highQuality = highQualityExportCheck.isSelected();
		logger.info("highQualityExport: " + highQuality);
		DIHelper.getInstance().getCoreProp().put(Constants.highQualityExport, highQuality);
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}

	
}
