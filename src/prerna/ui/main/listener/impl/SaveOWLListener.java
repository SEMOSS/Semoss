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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.GraphPlaySheet;

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
		saveIt();		
	}
	
	// temporary placeholder
	// once complete, this will be written into the prop file back
	/**
	 * Method saveIt.  Saves the configuration of the engine based on the graph play sheet.
	 */
	public void saveIt()
	{
//		GraphPlaySheet ps = (GraphPlaySheet)QuestionPlaySheetStore.getInstance().getActiveSheet();
		GraphPlaySheet ps = (GraphPlaySheet) ((OldInsight) InsightStore.getInstance().getActiveInsight()).getPlaySheet();
		String engineName = ps.engine.getEngineId();
		// get the core properties
		ps.exportDB();
		((AbstractDatabaseEngine)ps.engine).saveConfiguration();
	}
	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}
}
