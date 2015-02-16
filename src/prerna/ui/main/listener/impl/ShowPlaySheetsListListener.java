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

import java.beans.PropertyVetoException;
import java.util.Hashtable;

import javax.swing.JInternalFrame;
import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.util.QuestionPlaySheetStore;

/**
 * Controls the selection of a play sheet from the list of play sheets.
 */
public class ShowPlaySheetsListListener implements ListSelectionListener{

	ButtonMenuDropDown btnMenu;
	Hashtable<String,String> lookUp;
	
	/**
	 * Constructor for ShowPlaySheetsListListener.
	 */
	public ShowPlaySheetsListListener(){
	}
	
	/**
	 * Method setButtonMenu.  Sets the button menu that the listener will access.
	 * @param bt ButtonMenuDropDown
	 */
	public void setButtonMenu(ButtonMenuDropDown bt){
		this.btnMenu = bt;
	}
	
	/**
	 * Method setLookUpHash.  Sets the lookup hashtable that the listener will access.
	 * @param lookUp Hashtable<String,String>
	 */
	public void setLookUpHash(Hashtable<String,String> lookUp)
	{
		this.lookUp = lookUp;
	}
	
	/**
	 * Method valueChanged.  Retrieves information for a play sheet when it is selected from the list.
	 * @param e ListSelectionEvent
	 */
	@Override
	public void valueChanged(ListSelectionEvent e) {
		JList list = (JList) e.getSource();
		String listEntry = (String) list.getSelectedValue();
		
		String id = lookUp.get(listEntry);
		
		IPlaySheet selectedSheet = QuestionPlaySheetStore.getInstance().get(id);
		try {
			((JInternalFrame)selectedSheet).setSelected(true);
		} catch (PropertyVetoException e1) {
//			logger.error
			e1.printStackTrace();
		}
		QuestionPlaySheetStore.getInstance().setActiveSheet(selectedSheet);
		btnMenu.popupMenu.setVisible(false);

	}


}
