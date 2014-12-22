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
