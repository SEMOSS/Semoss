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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.EAPropertyCreator;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class EAPropertyCreatorListener extends AbstractListener {
	static final Logger LOGGER = LogManager.getLogger(EAPropertyCreatorListener.class.getName());
	
	public final String hrCoreDBName = "HR_Core";
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		// int response = JOptionPane.showOptionDialog(playPane, "This move cannot be undone.\n\n" +
		// "Would you still like to continue?\n",
		// "Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
		//
		// JOptionPane.showMessageDialog(playPane, "The Cost DB Loading Sheet can be found here:\n\n" + file);
		//
		// if (response == 1)
		// {
		IEngine hrCoreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDBName);
		
		LOGGER.info("Adding EA properties to " + hrCoreDBName);
		
		EAPropertyCreator creator = new EAPropertyCreator(hrCoreDB);
		try {
			creator.addProperties();
			Utility.showMessage("EA properties have been added to HR_Core!");
		} catch (EngineException e) {
			Utility.showError("Error with generating new DB. Make sure HR_Core properly defined.");
			e.printStackTrace();
		} catch (RepositoryException e) {
			Utility.showError("Error with generating new DB");
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			Utility.showError("Error with generating new DB");
			e.printStackTrace();
		}
		// }
	}
	
	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}
	
}
