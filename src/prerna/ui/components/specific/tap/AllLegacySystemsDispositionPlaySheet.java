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
package prerna.ui.components.specific.tap;

import java.util.Hashtable;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class AllLegacySystemsDispositionPlaySheet extends AbstractRDFPlaySheet{

	static final Logger logger = LogManager.getLogger(AllLegacySystemsDispositionPlaySheet.class.getName());
	private String checkModPropQuery = "ASK WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost> AS ?contains) {?p ?contains ?prop ;} }";
	private String modPropDeleteQuery = "DELETE { ?system ?contains ?prop } WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost> AS ?contains) {?p ?contains ?prop ;} }";

	@Override
	public void createData() {
		boolean modernizationPropExists = checkModernizationProp();
		if(!modernizationPropExists) 
		{
			// show continue popup
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			Object[] buttons = {"Cancel", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store does not " +
					"contain necessary calculated values.  Would you like to calculate now?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			// if user chooses to run insert, run the thing
			if(response == 1)
			{
				try {
					runModernizationPropInsert();
				} catch (EngineException e) {
					Utility.showError(e.getMessage());
					e.printStackTrace();
				}
			}
			else{
				return;
			}
		}
		else
		{
			// show override popup
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			Object[] buttons = {"Continue with stored values", "Recalculate"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains calculated values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			// if user chooses to overwrite, delete then insert
			if(response == 1)
			{
				deleteModernizationProp();
				try {
					runModernizationPropInsert();
				} catch (EngineException e) {
					Utility.showError(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		
		AllLegacySystemsDispositionProcessor processAllReports = new AllLegacySystemsDispositionProcessor();
		try {
			processAllReports.processReports();
		} catch (EngineException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		} catch (FileReaderException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
		
		Utility.showMessage("Successfully created all reports!");
	}
	
	private void runModernizationPropInsert() throws EngineException {
		InsertInterfaceModernizationProperty inserter = new InsertInterfaceModernizationProperty();
		inserter.insert();
	}
	
	private boolean checkModernizationProp(){
		logger.info("Checking modernization prop");
		boolean exists = false;

		BooleanProcessor proc = new BooleanProcessor();
		proc.setEngine(this.engine);
		proc.setQuery(checkModPropQuery);
		exists = proc.processQuery();
		logger.info("Modernization prop exists: " + exists);
		return exists;
	}
	
	private void deleteModernizationProp() {
		logger.info("Deleting modernization prop");
		UpdateProcessor upProc = new UpdateProcessor();
		upProc.setEngine(this.engine);
		upProc.setQuery(modPropDeleteQuery);
		upProc.processQuery();
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub

	}
}