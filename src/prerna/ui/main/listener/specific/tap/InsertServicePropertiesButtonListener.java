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

import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.BVCalculationPerformer;
import prerna.ui.components.specific.tap.ServiceICDProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Listener for btnInsertServiceProperties
 * Inserts service business value into selected database 
 */
public class InsertServicePropertiesButtonListener implements IChakraListener{

	String type = "Service";
	
	/**
	 * Inserts service business value when user presses btnInsertServiceProperties
	 * Calls BVCalculationPerformer and ServiceICDProcessor to insert service business value into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {

		
		/*The steps that this class will take:
		 * Check if bv or icd count exist on services
		 * Delete existing properties
		 * Run Business Calc
		 * Insert Business Calc
		 * Run ICD count
		 * Insert ICD count
		*/

		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }";
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(distanceQuery);
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		boolean distanceExists = proc.processQuery();
		if(distanceExists){
			//display message
			//if they want to continue--delete
			Object[] buttons = {"Cancel Calculation", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains inserted service values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

			if (response == 1){
				UpdateProcessor upProc = new UpdateProcessor();
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				
				BVCalculationPerformer bv = new BVCalculationPerformer();
				bv.setType(type);
				bv.runCalculation();

				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/ICDCount> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();
				ServiceICDProcessor serProc = new ServiceICDProcessor();
				serProc.runProcessor();
			}
		}
		else{
			BVCalculationPerformer bv = new BVCalculationPerformer();
			bv.setType(type);
			bv.runCalculation();

			ServiceICDProcessor serProc = new ServiceICDProcessor();
			serProc.runProcessor();
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
