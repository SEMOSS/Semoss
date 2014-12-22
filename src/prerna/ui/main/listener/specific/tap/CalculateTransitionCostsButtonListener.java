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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.TransitionCostInsert;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for calculateTransitionCostsButton on the MHS TAP tab
 * Inserts base transition estimates for TAP Cost database
 */
public class CalculateTransitionCostsButtonListener implements IChakraListener {
	
	static final Logger logger = LogManager.getLogger(CalculateTransitionCostsButtonListener.class.getName());
	SesameJenaSelectWrapper selectWrapper;
	GridFilterData gfd = new GridFilterData();
	JTable table = null;
	
	
	/**
	 * This is executed when the calculateTransitionCostsButton is pressed by the user
	 * Inserts base transition estimates for TAP Cost database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		logger.info("Transition Costs button pressed");
		//Get which radio button is selected
		JRadioButton overheadRadio = (JRadioButton) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_APPLY_OVERHEAD_RADIO);
		boolean overhead=false;
		if (overheadRadio.isSelected()){
			overhead = true;
		}
		
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		String loeCalcCheckquery = "SELECT ?s ?p ?o WHERE{ BIND(<http://semoss.org/ontologies/Relation/Contains/LOEcalc> AS ?p) {?s ?p ?o}}";

		boolean check = Utility.runCheck(loeCalcCheckquery); //check to see if the property LOEcalc already exists in the database.
		if (check == false){
			String glItemCheckQuery = "SELECT ?s ?p ?o WHERE{ BIND(<http://semoss.org/ontologies/Concept/TargetPhaseBasisSubTaskComplexityComplexity> AS ?o) {?s ?p ?o}}";
			boolean dbCheck = Utility.runCheck(glItemCheckQuery);
			if(dbCheck == false){
				JOptionPane.showMessageDialog(playPane, "The selected RDF store does not contain the necessary baseline estimates \nto " +
						"perfrom the calculation.  Please select a different RDF store and try again.", 
						"Error", JOptionPane.ERROR_MESSAGE);
				
			}
			else {
				runInsert(false, overhead);
			}
		}
		else{
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains calculated transition values.  Would you like to continue?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				runInsert(true, overhead);
			}
		}
	}
	
	/**
	 * Insert base transition elements for TAP Cost database
	 * @param delete 	boolean to see if database already contains base transition estimates
	 * @param overhead	boolean to see if user wants to apply TAP overhead in calculation
	 */
	public void runInsert(boolean delete, boolean overhead){
		String query = "";
		if(overhead) {
			query = DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_WITH_OVERHEAD);
		} else {
			query =DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_WITHOUT_OVERHEAD);
		}
		query += Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_SITEGLITEM);
		if(delete) {
			query=DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_DELETE) + Constants.TRANSITION_QUERY_SEPARATOR+query;
		}
		logger.info(query);
		TransitionCostInsert insert = new TransitionCostInsert();
		insert.setQuery(query);
		Runnable playRunner = null;
		playRunner = insert;
		// thread
		Thread playThread = new Thread(playRunner);
		playThread.start();
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	
	}
}
