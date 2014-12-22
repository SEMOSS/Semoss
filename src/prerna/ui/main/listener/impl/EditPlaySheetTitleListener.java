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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import prerna.ui.components.playsheets.AbstractRDFPlaySheet;

/**
 * Controls editing of the title of a playsheet in the internal frame.
 */
public class EditPlaySheetTitleListener implements ActionListener {

	AbstractRDFPlaySheet playSheet;
	
	/**
	 * Method setPlaySheet.  Sets the playsheet that the listener will access.
	 * @param playSheet AbstractRDFPlaySheet
	 */
	public void setPlaySheet(AbstractRDFPlaySheet playSheet){
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
