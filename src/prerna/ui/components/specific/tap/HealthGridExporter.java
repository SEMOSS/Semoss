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

import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.CONUSMapPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.main.listener.impl.ChartImageExportListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * This allows the application health grid to be exported.
 */
public class HealthGridExporter {

	static final Logger logger = LogManager.getLogger(HealthGridExporter.class.getName());
	
	/**
	 * Constructor for HealthGridExporter.
	 */
	public HealthGridExporter()
	{
	}
	
	/**
	 * Goes through system categories and health grid categories and creates new health grid if it is a new category.
	 * Sets up the layout and query to create the grid.
	 * Resends data to the health grid to highlight the appropriate system.
	 * Sets the location for exporting the application health grid for future use.
	 * @param systemList 	List of systems.
	 * @param catHash 		HashMap<String,String> of different categories.
	 */
	public void processData(ArrayList<String> systemList)
	{	
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		String id = "Health_Grid";
		String question = QuestionPlaySheetStore.getInstance().getIDCount() + ". "+id;
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
		//String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } ORDER BY ?System BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}"; //use if pulling central systems
		
		HealthGridSheet playSheet = new HealthGridSheet();					
		playSheet.setQuery(query);
		playSheet.setRDFEngine((IEngine) engine);
		playSheet.setQuestionID(question);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);

		QuestionPlaySheetStore.getInstance().put(question, playSheet);
		
		playSheet.setSystemHighlight(true);
		playSheet.setSystemToHighlight("ABACUS");
		playSheet.createData();
		playSheet.runAnalytics();
		playSheet.createView();
				
		boolean shouldStart = true;
		for(String system: systemList)
		{
			//set shouldStart to true in line 86 to run all systems.
			//if crashes on export, set shouldStart in line 86 to false.
			//Then put system equals to name of system it ended on and rerun.
			//will continue with the export for this system, and any after it.
			if(system.equals("TRAC2ES"))
				shouldStart = true;
			if(shouldStart)
			{
		query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } ORDER BY ?System BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
			//query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } ORDER BY ?System BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}"; //use if pulling central systems
			query = query.replace("ABACUS",system);
			playSheet.setQuery(query);
			playSheet.setSystemHighlight(true);
			playSheet.setSystemToHighlight("ABACUS");	
			playSheet.createData();
			playSheet.runAnalytics();
			playSheet.createView();



			if(!playSheet.isEmpty())
			{
				//location of export
				String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
				String folder = "\\export\\Images\\";
				String writeFileName = system.replaceAll(":","")+"_Health_Grid_Export.png";
				String fileLoc = workingDir + folder+writeFileName;
				
				//call chartimageexportlistener to export the app health grid.
				ChartControlPanel chartControl= playSheet.getControlPanel();
				JButton btnImageExport = chartControl.getImageExportButton();
				ActionListener[] actionList = btnImageExport.getActionListeners();
				ChartImageExportListener chartList = (ChartImageExportListener)actionList[0];
				chartList.setAutoExport(true);
				chartList.setFileLoc(fileLoc);
				chartList.setScaleBool(true);
				chartList.setScale(750,425);
				chartList.setCropBool(true);
				chartList.setCrop(27,100,435,295);
				btnImageExport.doClick();
				
//				try {
//					playSheet.setClosed(true);
//				} catch (PropertyVetoException e) {
//					e.printStackTrace();
//				}

			}
			playSheet.clearTables();

		}}
	}
}
