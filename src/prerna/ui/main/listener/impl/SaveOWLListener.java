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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.QuestionPlaySheetStore;

/**
 * Controls the saving of the OWL file.
 */
public class SaveOWLListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(SaveOWLListener.class.getName());
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		System.err.println("Saving the OWL");
		GraphPlaySheet ps = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();
		saveIt();		
	}
	
	// temporary placeholder
	// once complete, this will be written into the prop file back
	/**
	 * Method saveIt.  Saves the configuration of the engine based on the graph play sheet.
	 */
	public void saveIt()
	{
		GraphPlaySheet ps = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();
		String engineName = ps.engine.getEngineName();
		// get the core properties
		ps.exportDB();
		ps.engine.saveConfiguration();
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}
}
