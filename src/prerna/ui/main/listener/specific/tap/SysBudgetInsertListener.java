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
import javax.swing.JFrame;
import javax.swing.JOptionPane;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SystemBudgetPropInserter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnInsertBudgetProperty on MHS TAP
 * Inserts sustainment budget property on systems 
 */
public class SysBudgetInsertListener implements IChakraListener{

	//TODO: may want user to specify engine name
	
	String tapEngineName = "TAP_Core_Data";

	/**
	 * Prior to inserting sustainment budget property on systems, checks to see if the proper insert query exists in the question sheet of the selected database and to see if systems already contain the sustainment budget property
	 * Uses SystemBudgetPropInserter to modify the database
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
	
		String query = (String) DIHelper.getInstance().getProperty(Constants.SYSTEM_SUSTAINMENT_BUDGET_INSERT_QUERY);
		String budgetPropCheckQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> AS ?contains) {?s ?contains ?prop ;} }";
		
		if(query == null) {
			Utility.showError("Please select a database that contains budget information and try again");
		}
		else{
			BooleanProcessor boolProc = new BooleanProcessor();
			boolProc.setEngine((IEngine)DIHelper.getInstance().getLocalProp(tapEngineName));
			boolProc.setQuery(budgetPropCheckQuery);
			boolean propCheck = boolProc.processQuery();
			
			if(propCheck){
				//ask to overwrite

				JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
				Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
				int response = JOptionPane.showOptionDialog(playPane, "The active TAP_Core_Data store already " +
						"contains calculated budget values.  Would you like to overwrite?", 
						"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
				if(response ==1){
					//run delete
					String deleteQuery = "DELETE {?s ?contains ?prop. ?propContains ?type ?contains} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> AS ?propContains) BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type) BIND(<http://semoss.org/ontologies/Relation/Contains> AS ?contains) {?s ?propContains ?prop ;} {?propContains ?type ?contains}}";
					UpdateProcessor upProc = new UpdateProcessor();
					upProc.setEngine((IEngine)DIHelper.getInstance().getLocalProp(tapEngineName));
					upProc.setQuery(deleteQuery);
					upProc.processQuery();
					
					//run insert
					SystemBudgetPropInserter inserter = new SystemBudgetPropInserter();
					inserter.setQuery(query);
					inserter.runInsert();
				}
				else{
					//don't want to overwrite
					return;
				}
			}
			else {
				SystemBudgetPropInserter inserter = new SystemBudgetPropInserter();
				inserter.setQuery(query);
				inserter.runInsert();
			}
		}
	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
