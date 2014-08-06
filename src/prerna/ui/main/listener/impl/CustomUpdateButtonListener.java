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

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the Custom Insert/Delete query update button in the DB modification tab.
 */
public class CustomUpdateButtonListener implements IChakraListener{

	static final Logger logger = LogManager.getLogger(CustomUpdateButtonListener.class.getName());
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		int response = showWarning();
		if(response == 1)
		{
			JTextPane updateSparqlArea = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.UPDATE_SPARQL_AREA);
			//get the query
			String query = updateSparqlArea.getText();			
			//create UpdateProcessor class.  Set the query.  Let it run.
			UpdateProcessor processor = new UpdateProcessor();
			processor.setQuery(query);
			processor.processQuery();
		}
		
	}
	
	/**
	 * Method showWarning.	
	 * @return int */
	public int showWarning(){
		Object[] buttons = {"Cancel", "Continue"};
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		int response = JOptionPane.showOptionDialog(playPane, "The update query you are about to run \n" +
				"cannot be undone.  Would you like to continue?", 
				"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
		return response;
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}


}
