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
import javax.swing.JDesktopPane;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SysDecommissionOptimizationPlaySheet;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;


/**
 * Listener for sourceReportGenButton
 */
public class SysDecommissionOptimizationListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(SysDecommissionOptimizationListener.class.getName());

	/**
	 * This is executed when the btnFactSheetReport is pressed by the user
	 * Calls FactSheetProcessor to generate all the information from the queries to write onto the fact sheet
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {

		
		JTextField resourceTextField = (JTextField) DIHelper.getInstance().getLocalProp(ConstantsTAP.SYS_DECOM_OPT_RESOURCE_TEXT_FIELD);
		String resourceTextValue = resourceTextField.getText();
		Integer resourceValue = 0;
		JTextField timeTextField = (JTextField) DIHelper.getInstance().getLocalProp(ConstantsTAP.SYS_DECOM_OPT_TIME_TEXT_FIELD);
		String timeTextValue = timeTextField.getText();
		Double timeValue = 0.0;
		
		SysDecommissionOptimizationPlaySheet playsheet = new SysDecommissionOptimizationPlaySheet();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playsheet.setJDesktopPane(pane);
		QuestionPlaySheetStore.getInstance().customIDcount++;
		String playSheetTitle = "Custom Query - "+QuestionPlaySheetStore.getInstance().getCustomCount();
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+"custom";
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");

		QuestionPlaySheetStore.getInstance().put(insightID,  playsheet);
		playsheet.setRDFEngine(engine);
		playsheet.setQuestionID(insightID);
		playsheet.setTitle(playSheetTitle);
		
		String query="";
		if(resourceTextValue!=null&&resourceTextValue.length()>0)
		{
			query = "Constrain Resource";
			playsheet.setQuery(query);
			try{
				resourceValue = Integer.parseInt(resourceTextValue);
			}catch(RuntimeException e){
				Utility.showError("All text values must be numbers");
				return;
			}
			playsheet.runPlaySheet(query,resourceValue,0.0);
		}
		else if(timeTextValue!=null&&timeTextValue.length()>0)
		{
			query = "Constrain Time";
			playsheet.setQuery(query);
			try{
				timeValue = Double.parseDouble(timeTextValue)*365;
			}catch(RuntimeException e){
				Utility.showError("All text values must be numbers");
				return;
			}
			playsheet.runPlaySheet(query,0,timeValue);
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
