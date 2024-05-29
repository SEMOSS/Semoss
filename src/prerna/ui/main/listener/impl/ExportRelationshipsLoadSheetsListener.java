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
import java.util.ArrayList;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.poi.main.RelationshipLoadingSheetWriter;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls exporting of graph relationships into loading sheets.
 */
public class ExportRelationshipsLoadSheetsListener implements IChakraListener {
	Logger log = LogManager.getLogger(getClass());

	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		
		JComboBox exportDataSourceComboBox = (JComboBox) Utility.getDIHelperLocalProperty(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
		IDatabaseEngine engine = (IDatabaseEngine) Utility.getDIHelperLocalProperty(exportDataSourceComboBox.getSelectedItem().toString());
		
		ArrayList<String[]> relationships = new ArrayList<String[]>();
		for(int i = 1; i <= Constants.MAX_EXPORTS; i++) {
			JComboBox subject = (JComboBox) Utility.getDIHelperLocalProperty(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + i);
			JComboBox object = (JComboBox) Utility.getDIHelperLocalProperty(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + i);
			JComboBox relationship = (JComboBox) Utility.getDIHelperLocalProperty(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + i);
			if(subject.isVisible() && subject.getSelectedItem() != null && relationship.isVisible() && relationship.getSelectedItem() != null && 
					object.isVisible() && object.getSelectedItem() != null) {
				relationships.add(new String[]{subject.getSelectedItem().toString(), relationship.getSelectedItem().toString(), object.getSelectedItem().toString()});
			}
		}
		
		RelationshipLoadingSheetWriter writer = new RelationshipLoadingSheetWriter();
		writer.writeRelationshipLoadingSheets(engine, relationships);
	}

	
	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}
}
