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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JList;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

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
		} catch (RuntimeException ex){
			ex.printStackTrace();
			Utility.showError("Load has failed.");
		} catch (RepositoryException e) {
			e.printStackTrace();
			Utility.showError("Load has failed.");
		} catch (RDFHandlerException e) {
			e.printStackTrace();
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
