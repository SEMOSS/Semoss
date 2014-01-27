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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JList;

import prerna.ui.components.specific.tap.ActiveSystemUpdater;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnUpdateActiveSystems on the MHS TAP tab
 * Allow user to traverse freely only to active systems and not decommissioned systems
 */
public class UpdateActiveSystemsListener extends AbstractListener{

	/**
	 * This is executed when the btnUpdateActiveSystems is pressed by a user
	 * Calls UpdateActiveSystems class which does the processing to add the new triples into the engine
	 * Calls UpdateActiveSystems class to add new base relationships to the OWL file for the engine
	 * @param arg0
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		// select the TAP_Core database
		try{
			// get the correct engine user has selected to add the engine to
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object[] repos = (Object [])list.getSelectedValues();
			String engineName = (String) repos[0];
			// call UpdateActiveSystems to add the correct triples into the engine
			ActiveSystemUpdater updateSystemsClass = new ActiveSystemUpdater();
			updateSystemsClass.setEngine(engineName);
			updateSystemsClass.runUpdateActiveSystems();
			if(!updateSystemsClass.getFoundQuery()){
				Utility.showError("Could not find query!\nCheck that your Question Sheet has the correct queries.");
			}
			else{ // call UpdateActiveSystems to add the new base relationships to the OWL file
				updateSystemsClass.addToOWL(engineName);
				Utility.showMessage("Your database has been successfully updated!");
			}
		} catch (Exception ex){
			ex.printStackTrace();
			Utility.showError("Load has failed.");
		}
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {		
	}

}
