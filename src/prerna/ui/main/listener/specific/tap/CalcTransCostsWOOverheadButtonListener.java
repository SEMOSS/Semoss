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

//TODO: Class is no longer used. CalculateTransitionCostsButtonListener takes into consideration if TAP overhead radiobutton is pressed

/**
 */
public class CalcTransCostsWOOverheadButtonListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(CalcTransCostsWOOverheadButtonListener.class.getName());
	SesameJenaSelectWrapper selectWrapper;
	GridFilterData gfd = new GridFilterData();
	JTable table = null;
	
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		logger.info("Transition Costs button pressed");
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		String loeCalcCheckquery = "SELECT ?s ?p ?o WHERE{ BIND(<http://health.mil/ontologies/dRelation/Contains/LOEcalc> AS ?p) {?s ?p ?o}}";

		boolean check = Utility.runCheck(loeCalcCheckquery); //check to see if the property LOEcalc already exists in the database.
		if (check == false){
			String glItemCheckQuery = "SELECT ?s ?p ?o WHERE{ BIND(<http://semoss.org/ontologies/Concept/TargetPhaseBasisSubTaskComplexityComplexity> AS ?o) {?s ?p ?o}}";
			boolean dbCheck = Utility.runCheck(glItemCheckQuery);//check if engine contains estimates
			if(dbCheck == false){
				JOptionPane.showMessageDialog(playPane, "The selected RDF store does not contain the necessary baseline estimates \nto " +
						"perfrom the calculation.  Please select a different RDF store and try again.", 
						"Error", JOptionPane.ERROR_MESSAGE);
				
			}
			else {
				runInsert(false);
			}
		}
		else{
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains calculated transition values.  Would you like to overwrite?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				runInsert(true);
			}
			else {
			}
			
		}
	}
	
	
	
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
	 * Method runInsert.
	 * @param delete boolean
	 */
	public void runInsert(boolean delete){
		String query = null;
		if(delete==true) query=DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_DELETE)+
				Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_WITHOUT_OVERHEAD)
				+Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_SITEGLITEM);
		else if (delete==false) query=DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_WITHOUT_OVERHEAD)
				+Constants.TRANSITION_QUERY_SEPARATOR+
				DIHelper.getInstance().getProperty(Constants.TRANSITION_COST_INSERT_SITEGLITEM);
		TransitionCostInsert insert = new TransitionCostInsert();
		insert.setQuery(query);
		Runnable playRunner = null;
		playRunner = insert;
		// thread
		Thread playThread = new Thread(playRunner);
		playThread.start();
	}

	/**
	 * Method setView.
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}

	
}
