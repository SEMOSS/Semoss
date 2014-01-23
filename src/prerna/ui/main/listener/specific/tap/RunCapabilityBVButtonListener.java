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
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import prerna.algorithm.impl.DistanceDownstreamInserter;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.BVCalculationPerformer;
import prerna.ui.components.specific.tap.CapabilityBVCalculationPerformer;
import prerna.ui.components.specific.tap.FillTMHash;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnRunCapabilityBV
 * Determines business value calculations for a selected database
 */
public class RunCapabilityBVButtonListener implements IChakraListener{

	/**
	 * Performs business value when btnRunCapabilityBV is pressed by the user
	 * Calls CapabilityBVCalculationPerformer to insert business value calculations into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		/*The steps that this class will take:
		 * Check if BV already exists on capabilities
		 * if it does, have a popup
		 * if not, run bv for capabilities
		*/

		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/FutureBusinessValue> AS ?contains) {?s ?contains ?prop ;} }";
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
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/FutureBusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/RealizedBusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/FutureBusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/RealizedBusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				CapabilityBVCalculationPerformer bv = new CapabilityBVCalculationPerformer();
				bv.runCalculation();
			}
		}
		else{
			CapabilityBVCalculationPerformer bv = new CapabilityBVCalculationPerformer();
			bv.runCalculation();
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
