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
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import prerna.algorithm.impl.DistanceDownstreamInserter;
import prerna.engine.api.IEngine;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.BVCalculationPerformer;
import prerna.ui.components.specific.tap.FillTMHash;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

//TODO: should change name to RunBVTMButtonListner to be consistent with other naming

/**
 * Listener for btnInsertDownstream
 * Determines business value and technical maturity calculations for a selected database based on hardware and software data 
 */
public class InsertDownstreamButtonListener implements IChakraListener{

	/**
	 * Performs business value and technical maturity calculations when btnInsertDownstream is pressed by the user
	 * Calls BVCalculationPerformer and FillTMHash to insert business value and technical maturity calculations, respectively, into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JTextField soaTextField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.SOA_ALPHA_VALUE_TEXT_BOX);
		String soaTextValue = soaTextField.getText();
		Double soaAlphaValue = 0.0;
		JTextField appTextField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.APPRECIATION_TEXT_BOX);
		String appTextValue = appTextField.getText();
		Double appValue = 0.0;
		JTextField deptextField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.DEPRECIATION_TEXT_BOX);
		String depTextValue = deptextField.getText();
		Double depValue = 0.0;
		try{
			soaAlphaValue = Double.parseDouble(soaTextValue);
			appValue = Double.parseDouble(appTextValue);
			depValue = Double.parseDouble(depTextValue);
		}catch(RuntimeException e){
			Utility.showError("All text values must be numbers");
			return;
		}
		
		/*The steps that this class will take:
		 * Check if sys-IO weights exist
		 * (maybe popup to see if you want to recalculate weights)
		 * Delete existing weights
		 * Insert sys-IO weights
		 * Run Business Calc
		 * Insert Business Calc
		 * Run Tech Maturity
		 * Insert Tech Maturity
		*/

		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?s ?p ?o ;} BIND(<http://semoss.org/ontologies/Relation/Contains/DistanceDownstream> AS ?contains) {?p ?contains ?prop ;} }";
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(distanceQuery);
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		boolean distanceExists = proc.processQuery();
		if(distanceExists){
			//display message
			//if they want to continue--delete
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains inserted values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				UpdateProcessor upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?p ?contains ?prop} WHERE { {?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND (<http://semoss.org/ontologies/Relation/Contains/DistanceDownstream> AS ?contains) {?s ?p ?o ;} {?p ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.setQuery("DELETE {?p ?contains ?prop} WHERE { {?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND (<http://semoss.org/ontologies/Relation/Contains/weight> AS ?contains) {?s ?p ?o ;} {?p ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.setQuery("DELETE {?p ?contains ?prop} WHERE { {?o <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND (<http://semoss.org/ontologies/Relation/Contains/NetworkWeight> AS ?contains) {?s ?p ?o ;} {?p ?contains ?prop ;} }");
				upProc.processQuery();
				
				DistanceDownstreamInserter inserter = new DistanceDownstreamInserter();
				String eng = (String) ((JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST)).getSelectedValues()[0];
				inserter.setEngine((IEngine) DIHelper.getInstance().getLocalProp(eng));
				inserter.setAppAndDep(appValue, depValue);
				inserter.insertAllDataDownstream();

				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				upProc.processQuery();
			
				BVCalculationPerformer bv = new BVCalculationPerformer();
				String type = "System";
				bv.setType(type);
				bv.setsoaAlphaValue(soaAlphaValue);
				bv.runCalculation();

				FillTMHash tmPlaysheet = new FillTMHash();
				tmPlaysheet.runQueries();
				
				bv=null;
				tmPlaysheet=null;
				System.gc();
			}
		}
		else{
			DistanceDownstreamInserter inserter = new DistanceDownstreamInserter();
			String eng = (String) ((JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST)).getSelectedValues()[0];
			inserter.setEngine((IEngine) DIHelper.getInstance().getLocalProp(eng));
			inserter.setAppAndDep(appValue, depValue);
			inserter.insertAllDataDownstream();
			
			BVCalculationPerformer bv = new BVCalculationPerformer();
			String type = "System";
			bv.setType(type);
			bv.setsoaAlphaValue(soaAlphaValue);
			bv.runCalculation();
			FillTMHash tmPlaysheet = new FillTMHash();
			tmPlaysheet.runQueries();
			
			bv=null;
			tmPlaysheet=null;
			System.gc();
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
