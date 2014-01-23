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
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import prerna.om.SEMOSSEdge;
import prerna.ui.components.GraphToTreeConverter;
import prerna.ui.components.playsheets.GraphPlaySheet;
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