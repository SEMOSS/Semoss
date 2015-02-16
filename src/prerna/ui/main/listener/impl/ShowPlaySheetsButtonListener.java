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
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.StringNumericComparator;
/**
 * Listener for the button that populates the list of current play sheets dropdown menu. 
 */
public class ShowPlaySheetsButtonListener implements ActionListener{

	/**
	 * Constructor for PlaySheetListener.
	 */
	public ShowPlaySheetsButtonListener(){
		
	}
	
    /**
     * Method actionPerformed. This method is called when the playsheet window selector is clicked.
     * It generates a list of all current playsheets from the QuestionPlaySheetStore and lists them
     * in a ButtonMenuDropDown so that the user can easily change the active playsheet.
     * @param e ActionEvent
     */
	@Override
	public void actionPerformed(ActionEvent e) {
		ButtonMenuDropDown btn = (ButtonMenuDropDown) e.getSource();
		btn.resetButton();		
		Hashtable<String,String> lookUp = new Hashtable<String,String>();		
		Set<String> playSheetIDs = QuestionPlaySheetStore.getInstance().getAllSheets();
		List<String> sortedIDs = new ArrayList<String>();
		sortedIDs.addAll(playSheetIDs);
		Collections.sort(sortedIDs, new StringNumericComparator());
		String[] IDArray = new String[playSheetIDs.size()];
		int count = 0;
		for(String id : sortedIDs)
		{
			if(id.indexOf(".")>=0)	{
				String listEntry = id.substring(0,id.indexOf("."))+". "+QuestionPlaySheetStore.getInstance().get(id).getTitle();
	//			else
	//				listEntry = QuestionPlaySheetStore.getInstance().get(id).getTitle();
				IDArray[count] = listEntry;
				lookUp.put(listEntry,id);
				count++;
			}
			else if (id.indexOf("c")>=0) 
			{
				String listEntry = id.substring(0,id.indexOf("c"))+". "+QuestionPlaySheetStore.getInstance().get(id).getTitle();
				IDArray[count] = listEntry;
				lookUp.put(listEntry,id);
				count++;
			}
			else
				QuestionPlaySheetStore.getInstance().remove(id);
		}

			
		ShowPlaySheetsListListener showPlaySheetsListener = new ShowPlaySheetsListListener();
		showPlaySheetsListener.setButtonMenu(btn);
		showPlaySheetsListener.setLookUpHash(lookUp);
		btn.setupPlaySheetList(IDArray, 250, 200);
		btn.list.addListSelectionListener(showPlaySheetsListener);

	}


}