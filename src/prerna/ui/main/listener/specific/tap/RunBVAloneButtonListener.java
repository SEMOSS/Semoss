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
import javax.swing.JTextField;

import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.BusinessValueInserter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnRunBVAlone
 * Determines business value calculations for a selected database based on hardware and software data
 */
public class RunBVAloneButtonListener implements IChakraListener{

	/**
	 * Performs business value calculations when btnRunBVAlone is pressed by the user
	 * Calls BVCalculationPerformer to insert business value calculations into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get soaAlphaValue
		JTextField textField1 = (JTextField) DIHelper.getInstance().getLocalProp(Constants.SOA_ALPHA_VALUE_TEXT_BOX);
		String soaTextValue = textField1.getText();

		Double soaAlphaValue = 0.0;
		try{
			soaAlphaValue = Double.parseDouble(soaTextValue);
		}catch(RuntimeException e){
			Utility.showError("Text value must be a number");
			return;
		}
		
		/*The steps that this class will take:
		 * Check if BV already exists
		 * if it does, have a popup
		 * if not, run bv
		*/

		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }";
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(distanceQuery);
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		boolean bvExists = proc.processQuery();
		if(bvExists){
			//display message
			//if they want to continue--delete
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains inserted business values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				UpdateProcessor upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				// BVCalculationPerformer bv = new BVCalculationPerformer();
				// String type = "System";
				// bv.setType(type);
				// bv.setsoaAlphaValue(soaAlphaValue);
				// bv.runCalculation();
				BusinessValueInserter bvInserter = new BusinessValueInserter();
				bvInserter.setsoaAlphaValue(soaAlphaValue);
				bvInserter.runCalculationsAndInsertions();
				
				Utility.showMessage("Business Function has been successfully added!");

			}
		}
		else{
			// BVCalculationPerformer bv = new BVCalculationPerformer();
			// String type = "System";
			// bv.setType(type);
			// bv.setsoaAlphaValue(soaAlphaValue);
			// bv.runCalculation();
			BusinessValueInserter bvInserter = new BusinessValueInserter();
			bvInserter.setsoaAlphaValue(soaAlphaValue);
			bvInserter.runCalculationsAndInsertions();
			
			Utility.showMessage("Business Function has been successfully added!");

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
