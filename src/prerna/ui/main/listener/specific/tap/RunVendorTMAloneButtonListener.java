/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import prerna.engine.api.IDatabase;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.FillTMVendorHash;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnCalculateVendorTMAlone
 * Determines technical maturity calculations for a selected database for vendor selection 
 */
public class RunVendorTMAloneButtonListener implements IChakraListener {

	/**
	 * Performs technical maturity calculations when btnCalculateVendorTMAlone is pressed by the user
	 * Calls FillTMVendorHash to insert technical maturity calculations into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		/*The steps that this class will take:
		 * check if tm already exists
		 * if yes, show popup
		 * if no, run tm
		 */
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		String repo = repos[0] +"";
		IDatabase engine = (IDatabase)DIHelper.getInstance().getLocalProp(repo);
		if(!repo.toLowerCase().contains("vendor"))
		{
			Utility.showError("The database does not contain the required elements");
			return;
		}
		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> AS ?contains) {?s ?contains ?prop ;} }";
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(distanceQuery);
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		boolean distanceExists = false;
		try {
			distanceExists = proc.processQuery();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String distanceQuery2 = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> AS ?contains) {?s ?contains ?prop ;} }";
		proc = new BooleanProcessor();
		proc.setQuery(distanceQuery2);
		try {
			distanceExists = distanceExists || proc.processQuery();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(distanceExists){
			//display message
			//if they want to continue--delete
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains inserted TM values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				UpdateProcessor upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();

				FillTMVendorHash tmPlaysheet = new FillTMVendorHash();
				tmPlaysheet.runQueries();
			}
		}
		else{
			FillTMVendorHash tmPlaysheet = new FillTMVendorHash();
			tmPlaysheet.runQueries();
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
