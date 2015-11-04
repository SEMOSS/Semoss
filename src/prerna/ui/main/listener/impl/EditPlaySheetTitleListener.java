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
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import prerna.ui.components.playsheets.AbstractPlaySheet;

/**
 * Controls editing of the title of a playsheet in the internal frame.
 */
public class EditPlaySheetTitleListener implements ActionListener {

	AbstractPlaySheet playSheet;
	
	/**
	 * Method setPlaySheet.  Sets the playsheet that the listener will access.
	 * @param playSheet AbstractRDFPlaySheet
	 */
	public void setPlaySheet(AbstractPlaySheet playSheet){
		this.playSheet = playSheet;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		String newTitle = (String) JOptionPane.showInputDialog(
				playSheet,
				"Please enter the new title you wish to use:                                                           \n", 
				"Edit Play Sheet Title", 
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
				playSheet.getTitle());
		if(newTitle!=null)
			playSheet.setTitle(newTitle);
	}

}
