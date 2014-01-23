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
