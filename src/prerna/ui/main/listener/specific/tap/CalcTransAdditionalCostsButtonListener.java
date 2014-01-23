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
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTable;

import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.TransitionCostInsert;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for calcTransAdditionalButton on the MHS TAP tab
 * Inserts Semantics, Training, and Sustainment GLItems for TAP Cost database
 */
public class CalcTransAdditionalCostsButtonListener implements IChakraListener {
	Logger logger = Logger.getLogger(getClass());
	SesameJenaSelectWrapper selectWrapper;
	GridFilterData gfd = new GridFilterData();
	JTable table = null;
	
	
	/**
	 * This is executed when the calcTransAdditionalButton is pressed by the user
	 * Insert Semantics, Training, and Sustainment GLItems for TAP Cost database
	 * Requires user to press calculateTransitionCostsButton prior for this method to work
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Transition Costs button pressed");
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		String loeCalcCheckquery = "SELECT ?s ?p ?o WHERE{ BIND(<http://semoss.org/ontologies/Relation/Contains/LOEcalc> AS ?p) {?s ?p ?o}}";

		boolean check = Utility.runCheck(loeCalcCheckquery); //check to see if the property LOEcalc already exists in the database.
		if (check == false){
				JOptionPane.showMessageDialog(playPane, "The selected database currently does not contain baseline calculations. \nto " +
						"Please first generate baseline calculations and then try again", 
						"Error", JOptionPane.ERROR_MESSAGE);
				
		}
		else{
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains calculated transition values.  Would you like add additional calculations?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				runInsert();
			}
			else {
			}
			
		}
	}
	
	//TODO: can we delete the method below?
	
	/*
	public void fillTable(){
		String [] names = selectWrapper.getVariables();
		ArrayList <String []> list = new ArrayList();
		
		gfd.setColumnNames(names);
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(selectWrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = selectWrapper.next();
				
				String [] values = new String[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = sjss.getVar(names[colIndex])+"";
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
			gfd.setDataList(list);
			GridTableModel model = new GridTableModel(gfd);
			table.setModel(model);
		} catch (Exception e) {
			logger.fatal(e);
		}
	}*/


	/**
	 * Insert Semantics, Training, and Sustainment GLItems for TAP Cost database
	 */
	public void runInsert(){
		String query = DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_SEMANTICS)
				+Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_TRAINING)
				+Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_SUSTAINMENT);
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
