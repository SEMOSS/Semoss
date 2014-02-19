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
import javax.swing.JDesktopPane;
import javax.swing.JList;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
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

	Logger logger = Logger.getLogger(getClass());
	
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
		}catch(Exception ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
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
