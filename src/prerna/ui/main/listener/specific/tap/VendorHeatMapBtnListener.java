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
import java.lang.reflect.InvocationTargetException;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;


/**
 */
public class VendorHeatMapBtnListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(VendorHeatMapBtnListener.class.getName());
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		String query = DIHelper.getInstance().getProperty(ConstantsTAP.VENDOR_HEAT_MAP_REQUIREMENTS_QUERY+"_1");
		if(query==null)
		{
			Utility.showError("The database does not contain the required elements");
			return;
		}
		
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");
		
		
		String layoutValue = "prerna.ui.components.specific.tap.VendorHeatMapSheet";
		IPlaySheet playSheet = null;
		Runnable playRunner = null;

		try
		{
			playSheet = (IPlaySheet)Class.forName(layoutValue).getConstructor(null).newInstance(null);
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		} catch (InstantiationException e) {
			e.printStackTrace();
			logger.fatal(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			logger.fatal(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			logger.fatal(e);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			logger.fatal(e);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			logger.fatal(e);
		}
		playSheet.setTitle("Question: " + "Vendor Heat Map");
		playSheet.setQuestionID("Vendor_Heat_Map");
		playSheet.setQuery(query);
		playSheet.setRDFEngine((IEngine)engine);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);
	
		// need to create the playsheet create runner and run it on new thread
		playRunner = new PlaysheetCreateRunner(playSheet);
		Thread playThread = new Thread(playRunner);
		playThread.start();

		Utility.showMessage("Your Vendor Heat Map can be viewed in the Display Pane.");
	}

	
	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
