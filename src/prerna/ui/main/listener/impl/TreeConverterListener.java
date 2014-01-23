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

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JToggleButton;

import prerna.ui.components.GraphToTreeConverter;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * Controls converting the graph to a tree layout.
 */
public class TreeConverterListener implements ActionListener{

	GraphPlaySheet playSheet;
	GraphToTreeConverter converter;
	public DelegateForest networkForest;
	
	/**
	 * Constructor for TreeConverterListener.
	 */
	public TreeConverterListener(){
		converter = new GraphToTreeConverter();
	}
	
	/**
	 * Method setPlaySheet.  Sets the play sheet that the listener will access.
	 * @param ps GraphPlaySheet
	 */
	public void setPlaySheet(GraphPlaySheet ps){
		this.playSheet = ps;
		networkForest = ps.forest;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {		
		converter.setPlaySheet(playSheet);
		JToggleButton button = (JToggleButton) e.getSource();
		
		//if the button is selected run converter
		if(button.isSelected()){
			converter.execute();
		}
		//if the button is unselected, revert to old forest
		else{
			playSheet.setForest(networkForest);
		}
		boolean success = playSheet.createLayout();
		if(!success){
			int response = showOptionPopup();
			if(response == 1){
				playSheet.setLayout(Constants.FR);
				playSheet.createLayout();
			}
		}
		playSheet.refreshView();
		
	}

	/**
	 * Method showOptionPopup.	
	 * @return int */
	private int showOptionPopup(){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		Object[] buttons = {"Cancel Graph Modification", "Continue With "+Constants.FR};
		int response = JOptionPane.showOptionDialog(playPane, "This layout requires the graph to be in the format of a tree.\nWould you like to revert the layout to " + Constants.FR+ "?", 
				"Convert to Tree", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, buttons, buttons[1]);
		return response;
	}
}
