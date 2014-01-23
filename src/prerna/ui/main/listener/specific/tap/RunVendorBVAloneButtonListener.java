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

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.BVVendorCalculationPerformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Listener for btnCalculateVendorBVAlone
 * Determines business value calculations for a selected database for vendor selection 
 */
public class RunVendorBVAloneButtonListener implements IChakraListener{

	/**
	 * Performs business value calculations when btnCalculateVendorBVAlone is pressed by the user
	 * Calls BVVendorCalculationPerformer to insert business value calculations into the selected database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		/*The steps that this class will take:
		 * Check if BV already exists
		 * if it does, have a popup
		 * if not, run bv
		 */
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		String repo = repos[0] +"";
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
		if(!repo.toLowerCase().contains("vendor"))
		{
			Utility.showError("The database does not contain the required elements");
			return;
		}
		//Pass it to DistanceDownstreamInserter who will use DistanceDownstreamProcessor
		String distanceQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }";
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
				upProc.setQuery("DELETE {?s ?contains ?prop} WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/BusinessValue> AS ?contains) {?s ?contains ?prop ;} }");
				upProc.processQuery();

				BVVendorCalculationPerformer bv = new BVVendorCalculationPerformer();
				String type = "Vendor";
				bv.setType(type);
				bv.runCalculation();

			}
		}
		else{
			BVVendorCalculationPerformer bv = new BVVendorCalculationPerformer();
			String type = "Vendor";
			bv.setType(type);
			bv.runCalculation();
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
